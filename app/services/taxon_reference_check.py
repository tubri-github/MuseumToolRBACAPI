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


async def family_reference_warning(taxon_id):
    """Return a warning dict if the local taxon's family disagrees with the fish
    reference (genus->family), else None.

    Returns None when: no taxon, family-level placeholder taxon (no genus),
    reference not configured, the genus is unknown to the reference (non-fish /
    fossil / placeholder), or the family already agrees with the reference.
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

    return {
        "field": "Family",
        "issue_type": ISSUE_REFERENCE,
        "severity": "warning",
        "message": (
            f"Family '{local_fam}' for genus {genus} disagrees with the fish "
            f"reference ({', '.join(sorted(ref_fams))}). Please verify the taxon."
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
    if not vt or not (vt[0]["verbatim_family"] or "").strip():
        return None  # no source family -> nothing to contradict (blank is not "wrong")
    src_fam = vt[0]["verbatim_family"].strip()

    row = await execute_query(
        'SELECT f."FamilyName" FROM "TaxonomicTable" tt '
        'JOIN "Family" f ON tt."FamilyID" = f."FamilyID" WHERE tt."TaxonID" = $1',
        taxon_id,
    )
    if not row or not row[0]["FamilyName"]:
        return None
    matched_fam = row[0]["FamilyName"]
    if src_fam.lower() == matched_fam.lower():
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
