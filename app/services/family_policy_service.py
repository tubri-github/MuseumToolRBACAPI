"""
Family-classification rulings: one museum-wide decision per (local family, reference
family) disagreement, instead of the curator re-confirming the same taxonomic opinion on
every batch record.

Why this exists. taxon_reference_check compares a matched taxon's local family against
the CoF reference and, on disagreement, emits a warning that batch review turns into
species_verification_status='pending'. That guard is for real misfilings (Amia under
Acipenseridae). But CoF split the traditional Cyprinidae into Leuciscidae/Gobionidae/
Danionidae/..., which the museum has not adopted -- 743 of 825 local Cyprinidae taxa
"disagree", and at 20k records per batch that meant ~7078 forced-pending rows per batch
for a single unresolved opinion. A 'keep_local' ruling suppresses exactly that one pair.

This service only reads and writes the rulings + reports what is still undecided. It
never edits taxonomy: moving a family is a separate bulk operation that rewrites
TaxonomicTable."FamilyID" and touches no Determination, which is why 'adopt_reference' is
recordable as an intent but deliberately does NOT suppress the warning.

Rulings are revoked, never deleted -- who exempted what, and when, stays in the record.
"""
import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional

from app.db.database import execute_query, execute_single_query, execute_mutation
from app.db.taxon_database import execute_taxon_query, is_taxon_db_configured

logger = logging.getLogger("family_policy_service")

DECISIONS = ("keep_local", "adopt_reference")


class FamilyPolicyService:

    # ---- rulings ---------------------------------------------------------------------

    async def list_rulings(self, include_revoked: bool = False,
                           with_coverage: bool = True) -> List[Dict[str, Any]]:
        """Recorded rulings.

        With coverage (default) each ruling also carries how many taxa and specimens it is
        actually holding down. Without it the list reads like six lines of trivia; the whole
        point of a ruling is that one line covers hundreds of records
        (Cyprinidae->Leuciscidae alone: 702 taxa / ~72k specimens).
        """
        where = "" if include_revoked else " WHERE revoked_at IS NULL"
        rulings = await execute_query(
            "SELECT id, local_family, reference_family, decision, note, created_at, "
            "created_by, revoked_at, revoked_by, revoke_reason "
            f"FROM family_reference_policy{where} "
            "ORDER BY revoked_at NULLS FIRST, lower(local_family), lower(reference_family)")
        if not with_coverage or not rulings:
            return rulings

        try:
            all_pairs = await self.disagreements(include_covered=True)
        except Exception as e:  # noqa: BLE001 - coverage is a nicety, never break the list
            logger.warning("coverage lookup failed: %s", e)
            return rulings
        by_pair = {(p["local_family"].lower(), p["reference_family"].lower()): p
                   for p in all_pairs.get("pairs", [])}
        for r in rulings:
            p = by_pair.get((r["local_family"].lower(), r["reference_family"].lower()))
            r["taxa"] = p["taxa"] if p else 0
            r["determinations"] = p["determinations"] if p else 0
            r["sample_genera"] = p["sample_genera"] if p else []
        return rulings

    @staticmethod
    async def add_ruling(local_family: str, reference_family: str, decision: str,
                         note: Optional[str], created_by: str) -> Dict[str, Any]:
        local_family = (local_family or "").strip()
        reference_family = (reference_family or "").strip()
        if not local_family or not reference_family:
            return {"error": "local_family and reference_family are required"}
        if decision not in DECISIONS:
            return {"error": f"decision must be one of {DECISIONS}"}

        dup = await execute_single_query(
            "SELECT id FROM family_reference_policy WHERE revoked_at IS NULL "
            "AND lower(local_family)=lower($1) AND lower(reference_family)=lower($2)",
            local_family, reference_family)
        if dup:
            return {"error": f"an active ruling for {local_family} -> {reference_family} "
                             f"already exists (id {dup['id']}); revoke it first"}

        row = await execute_single_query(
            "INSERT INTO family_reference_policy "
            "(local_family, reference_family, decision, note, created_by) "
            "VALUES ($1,$2,$3,$4,$5) RETURNING id, local_family, reference_family, "
            "decision, note, created_at, created_by",
            local_family, reference_family, decision, note, (created_by or "")[:120])
        return dict(row) if row else {"error": "insert failed"}

    @staticmethod
    async def revoke_ruling(ruling_id: int, revoked_by: str,
                            reason: Optional[str] = None) -> Dict[str, Any]:
        row = await execute_single_query(
            "SELECT id, revoked_at FROM family_reference_policy WHERE id=$1", ruling_id)
        if not row:
            return {"error": "ruling not found"}
        if row["revoked_at"] is not None:
            return {"error": "already revoked"}
        await execute_mutation(
            "UPDATE family_reference_policy SET revoked_at=NOW(), revoked_by=$1, "
            "revoke_reason=$2 WHERE id=$3", (revoked_by or "")[:120], reason, ruling_id)
        return {"id": ruling_id, "status": "revoked"}

    # ---- the taxa behind one row ------------------------------------------------------

    @staticmethod
    async def affected_taxa(local_family: str, reference_family: str) -> Dict[str, Any]:
        """Every taxon behind one (local family -> reference family) row.

        The pair alone ("Triglidae vs Peristediidae") is not something anyone can judge; the
        curator needs to see WHICH of our taxa are involved and how many specimens hang off
        each, which is what makes the decision concrete.
        """
        if not is_taxon_db_configured():
            return {"error": "CoF reference DB is not configured"}

        local = await execute_query(
            'SELECT tt."TaxonID", btrim(tt."Genus") AS genus, tt."Species" AS species, '
            '       tt."FullScientificName" AS full_name, '
            '  (SELECT count(*) FROM "Determination" d '
            '   WHERE d."TaxonID" = tt."TaxonID" AND d."IsCurrent" IS TRUE) AS specimens '
            'FROM "TaxonomicTable" tt JOIN "Family" f ON f."FamilyID" = tt."FamilyID" '
            'WHERE lower(f."FamilyName") = lower($1) '
            '  AND tt."Genus" IS NOT NULL AND btrim(tt."Genus") <> \'\' '
            'ORDER BY tt."FullScientificName"', local_family)
        if not local:
            return {"items": [], "total": 0, "total_specimens": 0}

        genera = sorted({r["genus"].lower() for r in local})
        ref = await execute_taxon_query(
            "SELECT g.scientific_name AS genus, fam.scientific_name AS fam "
            "FROM taxa g JOIN taxa fam ON g.parent_id = fam.id AND fam.rank = 'FAMILY' "
            "WHERE g.rank = 'GENUS' AND g.status = 'valid' "
            "AND lower(g.scientific_name) = ANY($1::text[])", genera)
        g2f = defaultdict(set)
        for r in ref:
            g2f[r["genus"].lower()].add(r["fam"])

        items = [dict(r) for r in local
                 if reference_family in g2f.get(r["genus"].lower(), set())]
        return {
            "local_family": local_family,
            "reference_family": reference_family,
            "items": items,
            "total": len(items),
            "total_specimens": sum(i["specimens"] for i in items),
        }

    # ---- what is still undecided -----------------------------------------------------

    @staticmethod
    async def disagreements(include_covered: bool = False) -> Dict[str, Any]:
        """Every (local family -> reference family) disagreement in the taxon table, one row
        per DECISION (not per taxon), with how many taxa and specimens ride on it.

        Set-based on purpose: the per-taxon path costs a round trip each and this is a
        whole-table sweep.
        """
        if not is_taxon_db_configured():
            return {"error": "CoF reference DB is not configured"}

        local = await execute_query(
            'SELECT tt."TaxonID", btrim(tt."Genus") AS genus, f."FamilyName" AS fam, '
            '  (SELECT count(*) FROM "Determination" d '
            '   WHERE d."TaxonID" = tt."TaxonID" AND d."IsCurrent" IS TRUE) AS dets '
            'FROM "TaxonomicTable" tt JOIN "Family" f ON f."FamilyID" = tt."FamilyID" '
            'WHERE tt."Genus" IS NOT NULL AND btrim(tt."Genus") <> \'\' '
            '  AND f."FamilyName" IS NOT NULL')
        genera = sorted({r["genus"].lower() for r in local})
        if not genera:
            return {"pairs": [], "totals": {"taxa": 0, "determinations": 0, "decisions": 0}}

        ref = await execute_taxon_query(
            "SELECT g.scientific_name AS genus, fam.scientific_name AS fam "
            "FROM taxa g JOIN taxa fam ON g.parent_id = fam.id AND fam.rank = 'FAMILY' "
            "WHERE g.rank = 'GENUS' AND g.status = 'valid' "
            "AND lower(g.scientific_name) = ANY($1::text[])", genera)
        g2f = defaultdict(set)
        for r in ref:
            g2f[r["genus"].lower()].add(r["fam"])

        policy = {(r["local_family"].lower(), r["reference_family"].lower())
                  for r in await execute_query(
                      "SELECT local_family, reference_family FROM family_reference_policy "
                      "WHERE decision='keep_local' AND revoked_at IS NULL")}

        pairs: Dict[tuple, Dict[str, Any]] = {}
        covered_taxa = 0
        for r in local:
            refs = g2f.get(r["genus"].lower())
            if not refs or r["fam"].lower() in {f.lower() for f in refs}:
                continue
            for rf in sorted(refs):
                is_covered = (r["fam"].lower(), rf.lower()) in policy
                if is_covered:
                    covered_taxa += 1
                    if not include_covered:
                        continue
                key = (r["fam"], rf)
                p = pairs.setdefault(key, {"local_family": r["fam"], "reference_family": rf,
                                           "taxa": 0, "determinations": 0,
                                           "sample_genera": [], "covered": is_covered})
                p["taxa"] += 1
                p["determinations"] += r["dets"]
                if r["genus"] not in p["sample_genera"] and len(p["sample_genera"]) < 5:
                    p["sample_genera"].append(r["genus"])

        out = sorted(pairs.values(), key=lambda p: -p["determinations"])
        return {
            "pairs": out,
            "totals": {
                "taxa": sum(p["taxa"] for p in out),
                "determinations": sum(p["determinations"] for p in out),
                "decisions": len(out),
                "taxa_already_covered": covered_taxa,
            },
            # The reference is a snapshot loaded INSERT-only in two passes; the second pass
            # did not re-parent existing rows, so some "disagreements" can be an artifact of
            # how it was built rather than a real taxonomic opinion. Surfaced so the UI can
            # say so instead of presenting every row as a confirmed problem.
            "reference_caveat": (
                "The CoF reference is a snapshot loaded in two INSERT-only passes "
                "(2026-01-14, 2026-06-15); it does not re-parent taxa moved by a later "
                "split, so some disagreements may be artifacts of the snapshot."
            ),
        }