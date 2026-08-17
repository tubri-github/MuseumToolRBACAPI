"""
Reference cross-check for taxon review.

Batch review verifies a taxon against the LOCAL taxon db only; the fish reference
(TaxonRank = CAS Catalog of Fishes) is never consulted, so a locally-misfiled
family (e.g. Amia under Acipenseridae) gets "verified" silently and used to need a
corrective SQL script. This adds a non-blocking WARNING when the local taxon's
family disagrees with the reference's genus->family.

Advisory only: it writes into primary_temp.verification_warnings (severity
"warning"), never blocks the apply, and is best-effort (never raises to the caller).
The reference is a snapshot that can lag recent family splits (Dorosomatidae,
Alosidae, ...), so treat hits as "please double-check", not as hard errors.
"""
import json
import logging

from app.db.database import execute_query, execute_mutation
from app.db.taxon_database import execute_taxon_query, is_taxon_db_configured

logger = logging.getLogger("taxon_reference_check")

# Two complementary family checks, both written into verification_warnings:
#   reference  = matched taxon's family vs the external fish reference (catches a locally
#                misfiled taxon, e.g. Amia under Acipenseridae)  -> forces pending
#   suggestion = the imported (verbatim) family vs the matched taxon's family (catches a
#                wrong/reclassified source family, e.g. DOROSOMA filed as CYPRINIDAE but
#                matched to Clupeidae)                            -> warning only
ISSUE_REFERENCE = "family_reference_mismatch"
ISSUE_SUGGESTION = "family_suggestion_mismatch"
MANAGED = (ISSUE_REFERENCE, ISSUE_SUGGESTION)
ISSUE_TYPE = ISSUE_REFERENCE  # backward-compat alias


async def _policy_exempt(local_family, ref_families):
    """Drop the reference families the museum has already decided to ignore for this
    local family (family_reference_policy, decision='keep_local').

    Returns the families still in genuine disagreement. A pair that is not recorded as
    policy still disagrees, so a real misfiling is never masked. Best-effort: if the
    table is missing (migration not run), nothing is exempted.
    """
    if not ref_families:
        return set()
    try:
        rows = await execute_query(
            "SELECT reference_family FROM family_reference_policy "
            "WHERE decision = 'keep_local' AND revoked_at IS NULL "
            "AND lower(local_family) = lower($1) "
            "AND lower(reference_family) = ANY($2::text[])",
            local_family, [f.lower() for f in ref_families],
        )
    except Exception as e:  # noqa: BLE001 - advisory; never break the caller
        logger.warning("family_reference_policy lookup failed: %s", e)
        return set(ref_families)
    exempt = {r["reference_family"].lower() for r in rows}
    return {f for f in ref_families if f.lower() not in exempt}


async def family_reference_warning(taxon_id):
    """Return a warning dict if the local taxon's family disagrees with the fish
    reference (genus->family), else None.

    Returns None when: no taxon, family-level placeholder taxon (no genus),
    reference not configured, the genus is unknown to the reference (non-fish /
    fossil / placeholder), the family already agrees with the reference, or every
    disagreeing reference family is exempted by family_reference_policy (a
    classification opinion the museum has already ruled on -- see that migration).
    """
    if taxon_id is None or not is_taxon_db_configured():
        return None

    row = await execute_query(
        'SELECT tt."Genus", f."FamilyName" '
        'FROM "TaxonomicTable" tt JOIN "Family" f ON tt."FamilyID" = f."FamilyID" '
        'WHERE tt."TaxonID" = $1',
        taxon_id,
    )
    if not row:
        return None
    genus = (row[0]["Genus"] or "").strip()
    local_fam = row[0]["FamilyName"]
    if not genus or not local_fam:
        return None  # family-level placeholder taxon: nothing to cross-check

    ref = await execute_taxon_query(
        "SELECT fam.scientific_name AS f "
        "FROM taxa g JOIN taxa fam ON g.parent_id = fam.id AND fam.rank = 'FAMILY' "
        "WHERE g.rank = 'GENUS' AND g.status = 'valid' "
        "AND lower(g.scientific_name) = lower($1)",
        genus,
    )
    ref_fams = {r["f"] for r in ref if r["f"]}
    # case-insensitive: local family names are inconsistently cased (e.g. POTAMOTRYGONIDAE)
    if not ref_fams or local_fam.lower() in {f.lower() for f in ref_fams}:
        return None  # genus unknown to reference, or family agrees -> no warning

    # A disagreement the museum has already ruled on is not the curator's problem again.
    ref_fams = await _policy_exempt(local_fam, ref_fams)
    if not ref_fams:
        return None

    return {
        "field": "Family",
        "issue_type": ISSUE_REFERENCE,
        "severity": "warning",
        "message": (
            f"Family '{local_fam}' for genus {genus} disagrees with the fish "
            f"reference ({', '.join(sorted(ref_fams))}). Please verify the taxon."
        ),
    }


def build_suggestion_warning(src_fam, matched_fam):
    """The suggestion warning for one (imported family, matched family) pair, or None if
    there is nothing to complain about.

    Split out so the per-record path below and the name-group bulk path
    (app/services/name_group_service.py) produce the SAME message: a curator comparing one
    record against a group of 1300 must not see two different wordings for one condition.
    """
    src_fam = (src_fam or "").strip()
    matched_fam = (matched_fam or "").strip()
    # blank source family is not "wrong": there is simply nothing to contradict
    if not src_fam or not matched_fam or src_fam.lower() == matched_fam.lower():
        return None
    return {
        "field": "Family",
        "issue_type": ISSUE_SUGGESTION,
        "severity": "warning",
        "message": (
            f"Imported family '{src_fam}' differs from the matched taxon's family "
            f"'{matched_fam}'. Confirm the family is correct."
        ),
    }


async def family_suggestion_warning(record_id, taxon_id):
    """Return a warning dict if the IMPORTED (verbatim) family differs from the matched
    taxon's family -- i.e. the source family was wrong/reclassified by the match (e.g.
    DOROSOMA filed as CYPRINIDAE but matched to Clupeidae). Else None.

    Returns None when no taxon, no verbatim family on record (nothing to contradict),
    matched taxon has no family, or the two agree (case-insensitive).
    """
    if taxon_id is None:
        return None
    vt = await execute_query(
        'SELECT vt.verbatim_family FROM primary_temp pt '
        'JOIN verbatim_taxonomic vt ON pt.verbatim_taxonid = vt.verbatim_taxonid '
        'WHERE pt."PrimaryID" = $1',
        record_id,
    )
    if not vt:
        return None
    src_fam = vt[0]["verbatim_family"]

    row = await execute_query(
        'SELECT f."FamilyName" FROM "TaxonomicTable" tt '
        'JOIN "Family" f ON tt."FamilyID" = f."FamilyID" WHERE tt."TaxonID" = $1',
        taxon_id,
    )
    return build_suggestion_warning(src_fam, row[0]["FamilyName"] if row else None)


async def store_warnings(record_id, warnings):
    """Merge module-managed family warnings into verification_warnings, replacing any
    prior ones of the managed issue types. warnings = list (entries may be None).
    Best-effort: logged and swallowed, never raises.
    """
    try:
        cur = await execute_query(
            'SELECT verification_warnings FROM primary_temp WHERE "PrimaryID" = $1',
            record_id,
        )
        existing = []
        if cur and cur[0]["verification_warnings"]:
            try:
                existing = json.loads(cur[0]["verification_warnings"]) or []
            except (ValueError, TypeError):
                existing = []

        existing = [w for w in existing if w.get("issue_type") not in MANAGED]
        existing.extend([w for w in warnings if w])

        await execute_mutation(
            'UPDATE primary_temp SET verification_warnings = $1 WHERE "PrimaryID" = $2',
            json.dumps(existing) if existing else None,
            record_id,
        )
    except Exception as e:  # noqa: BLE001 - advisory; must never break the caller
        logger.warning("store_warnings failed for record %s: %s", record_id, e)


async def apply_family_checks(record_id, taxon_id):
    """Run BOTH family checks for an applied taxon, persist their warnings, and return
    them as {"reference": w|None, "suggestion": w|None}. Best-effort: never raises.
    """
    ref = sugg = None
    try:
        ref = await family_reference_warning(taxon_id)
    except Exception as e:  # noqa: BLE001
        logger.warning("family_reference_warning failed for %s: %s", record_id, e)
    try:
        sugg = await family_suggestion_warning(record_id, taxon_id)
    except Exception as e:  # noqa: BLE001
        logger.warning("family_suggestion_warning failed for %s: %s", record_id, e)
    await store_warnings(record_id, [ref, sugg])
    return {"reference": ref, "suggestion": sugg}
