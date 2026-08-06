"""
Whole-DB taxonomic check (rewrite of the taxon-review scan).

Checks the museum DB's in-use taxonomic names against the CoF reference and classifies
each into a single primary category (precedence: form first, then status), plus the
orthogonal FAMILY_MISMATCH flag. Results are written to taxon_synonym_review (reusing the
existing table / CRUD / frontend), to be reviewed and applied by the curator. The apply
step (determination write-back) lives in taxon_apply_service.

Design decisions (see memory taxon_recheck_rewrite):
  - Name source is FullScientificName, NOT the Genus column (Genus is corrupted for ~24% of
    rows). See memory taxonomictable_genus_corrupted.
  - Only IN-USE taxa are scanned (those with current determinations); usage_count is the
    Determination(IsCurrent) count, not the near-empty Primary.TaxonID.
  - The tool DETECTS and SUGGESTS; the curator DECIDES. Only clean-binomial EXACT_SYNONYM
    with a single authoritative target is marked `appliable` (one-click). Everything with a
    judgment call (HYBRID, TRINOMIAL, RECOMBINATION, multi-target) is surfaced with a flag
    and a suggestion but requires explicit curator target-selection.

Category precedence:
  HYBRID        name contains an ' x '/' X ' hybrid marker
  NO_BINOMIAL   genus-level / open nomenclature (sp., spp., cf., aff., or no epithet)
  TRINOMIAL     3+ meaningful tokens (subspecies)
  <clean binomial>:
    EXACT_VALID     full name is a valid CoF species          -> no action
    EXACT_SYNONYM   full name is a CoF synonym -> accepted      -> appliable if single target
    RECOMBINATION   name absent from CoF, same epithet exists elsewhere -> needs-manual
    NOT_IN_COF      name unknown to the fish reference          -> advisory
"""
import logging
import time
import uuid
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from app.db.database import execute_query, execute_mutation
from app.db.taxon_database import execute_taxon_query, is_taxon_db_configured

logger = logging.getLogger("taxon_check_service")

# open-nomenclature / placeholder epithets -> genus-level, not a binomial
PLACEHOLDER_SP = {"sp", "sp.", "spp", "spp.", "cf", "cf.", "aff", "aff.",
                  "indet", "indet.", "?", "nr", "nr.", "near"}
HYBRID_MARKERS = {"x", "×"}

# category constants
HYBRID = "HYBRID"
NO_BINOMIAL = "NO_BINOMIAL"
TRINOMIAL = "TRINOMIAL"
EXACT_VALID = "EXACT_VALID"
EXACT_SYNONYM = "EXACT_SYNONYM"
RECOMBINATION = "RECOMBINATION"
NOT_IN_COF = "NOT_IN_COF"


class TaxonCheckService:
    """Read-only scan engine + persistence into taxon_synonym_review."""

    _cof: Optional[Dict[str, Any]] = None
    _cof_ts: Optional[float] = None
    _cof_ttl: int = 3600

    # ---- CoF reference indices -------------------------------------------------------------

    async def load_cof(self, force: bool = False) -> Dict[str, Any]:
        now = time.time()
        if (not force and self._cof is not None and self._cof_ts is not None
                and now - self._cof_ts < self._cof_ttl):
            return self._cof
        if not is_taxon_db_configured():
            raise RuntimeError("CoF reference DB not configured (set TAXON_DB_* in .env)")

        species = await execute_taxon_query(
            "SELECT t.scientific_name, t.status, "
            "COALESCE(v.scientific_name, t.scientific_name) AS valid_name "
            "FROM taxa t LEFT JOIN taxa v ON t.valid_id = v.id WHERE t.rank='SPECIES'"
        )
        by_full: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {"has_valid": False, "syn_targets": set()})
        by_epithet: Dict[str, list] = defaultdict(list)
        for r in species:
            p = (r["scientific_name"] or "").strip().lower().split()
            if len(p) < 2:
                continue
            k = f"{p[0]} {p[1]}"
            if r["status"] == "synonym":
                by_full[k]["syn_targets"].add(r["valid_name"])
                by_epithet[p[1]].append({"name": r["scientific_name"], "valid": r["valid_name"]})
            else:
                by_full[k]["has_valid"] = True

        # genus_lower -> {family names} from valid GENUS rows (parent_id chain, homonym-safe)
        genus_family: Dict[str, set] = defaultdict(set)
        gf = await execute_taxon_query(
            "SELECT g.scientific_name AS genus, fam.scientific_name AS family "
            "FROM taxa g JOIN taxa fam ON g.parent_id = fam.id AND fam.rank='FAMILY' "
            "WHERE g.rank='GENUS' AND g.status='valid'"
        )
        for r in gf:
            if r["genus"] and r["family"]:
                genus_family[r["genus"].strip().lower()].add(r["family"].strip())

        self._cof = {"by_full": dict(by_full), "by_epithet": dict(by_epithet),
                     "genus_family": dict(genus_family), "species_count": len(species)}
        self._cof_ts = now
        logger.info("CoF loaded: %d species, %d epithets, %d genera",
                    len(by_full), len(by_epithet), len(genus_family))
        return self._cof

    # ---- name parsing + classification -----------------------------------------------------

    @staticmethod
    def parse_full(full: str) -> Dict[str, Any]:
        """Genus/species/subspecies from FullScientificName. Detects hybrid markers and
        open-nomenclature placeholders. Author/year tokens after the epithet are ignored."""
        toks = [t for t in (full or "").strip().split() if t]
        low = [t.lower() for t in toks]
        is_hybrid = any(t in HYBRID_MARKERS for t in low)
        if len(toks) < 2:
            return {"genus": toks[0] if toks else "", "species": "", "subspecies": "",
                    "is_hybrid": is_hybrid, "n_meaningful": len(toks)}
        genus = toks[0]
        species = toks[1]
        if not species.isalpha() or species.lower() in PLACEHOLDER_SP:
            return {"genus": genus, "species": "", "subspecies": "",
                    "is_hybrid": is_hybrid, "n_meaningful": 1}
        # subspecies = a 3rd alpha token that is not a placeholder / author (heuristic: lowercase alpha)
        subspecies = ""
        if len(toks) >= 3 and toks[2].isalpha() and toks[2].islower() \
                and toks[2].lower() not in PLACEHOLDER_SP:
            subspecies = toks[2]
        return {"genus": genus, "species": species, "subspecies": subspecies,
                "is_hybrid": is_hybrid, "n_meaningful": 3 if subspecies else 2}

    def _classify_binomial(self, cof, genus: str, species: str) -> Tuple[str, str, List[str]]:
        """Classify a clean binomial -> (category, single_target|'', candidates)."""
        g = (genus or "").strip().lower()
        s = (species or "").strip().lower()
        if not g or not s:
            return (NO_BINOMIAL, "", [])
        full = f"{g} {s}"
        hit = cof["by_full"].get(full)
        if hit and hit["has_valid"]:
            return (EXACT_VALID, "", [])
        if hit and hit["syn_targets"]:
            tg = sorted(t for t in hit["syn_targets"] if t and t.lower() != full)
            return (EXACT_SYNONYM, tg[0] if len(tg) == 1 else "", tg)
        raw = sorted({c["valid"] for c in cof["by_epithet"].get(s, [])
                      if c["valid"] and c["name"].split()[0].lower() != g})
        if raw:
            return (RECOMBINATION, "", raw)  # never one-click: homonym-epithet risk
        return (NOT_IN_COF, "", [])

    def classify(self, cof, full_name: str) -> Dict[str, Any]:
        """Return the primary category + suggestion + candidates for a FullScientificName."""
        p = self.parse_full(full_name)
        genus, species, sub = p["genus"], p["species"], p["subspecies"]

        if p["is_hybrid"]:
            return {"category": HYBRID, "suggested": "", "candidates": [],
                    "genus": genus, "species": species, "subspecies": sub}
        if not species:
            return {"category": NO_BINOMIAL, "suggested": "", "candidates": [],
                    "genus": genus, "species": "", "subspecies": ""}
        if sub:
            # classify the binomial part; carry its verdict as a *suggestion* (curator confirms)
            cat, target, cands = self._classify_binomial(cof, genus, species)
            return {"category": TRINOMIAL, "suggested": target, "candidates": cands,
                    "binomial_status": cat, "genus": genus, "species": species, "subspecies": sub}
        cat, target, cands = self._classify_binomial(cof, genus, species)
        return {"category": cat, "suggested": target, "candidates": cands,
                "genus": genus, "species": species, "subspecies": ""}

    @staticmethod
    async def _load_family_policy() -> set:
        """The (local family, reference family) pairs the museum has already ruled on.

        The same disagreement is surfaced by two code paths -- batch review via
        taxon_reference_check, and this scan -- and a ruling has to silence both. Before this
        was wired up, 623 of 948 pending reviews were family pairs that had already been
        settled (Cyprinidae->Leuciscidae alone accounted for 584), i.e. two thirds of the
        curator's backlog was noise.
        """
        try:
            rows = await execute_query(
                "SELECT local_family, reference_family FROM family_reference_policy "
                "WHERE decision = 'keep_local' AND revoked_at IS NULL")
            return {(r["local_family"].lower(), r["reference_family"].lower()) for r in rows}
        except Exception as e:  # noqa: BLE001 - a missing table must not break the scan
            logger.warning("family policy not available, scanning without it: %s", e)
            return set()

    @staticmethod
    def _family_mismatch(cof, genus: str, local_family: Optional[str],
                         policy: Optional[set] = None) -> Tuple[bool, str]:
        """True + reference-family string when the local family disagrees with the CoF
        genus->family reference. Silent (False) when the genus is unknown to the reference,
        the family agrees, or every disagreeing family has been ruled 'keep_local'.

        Exemption is per reference family: if CoF returns several and only some are covered,
        the rest are still reported, so one ruling cannot mask an unrelated disagreement.
        """
        g = (genus or "").strip().lower()
        if not g or not local_family:
            return (False, "")
        ref = cof["genus_family"].get(g)
        if not ref:
            return (False, "")  # non-fish / fossil / placeholder genus
        lf = local_family.strip().lower()
        if lf in {f.lower() for f in ref}:
            return (False, "")
        if policy:
            ref = {f for f in ref if (lf, f.lower()) not in policy}
            if not ref:
                return (False, "")
        return (True, "; ".join(sorted(ref)))

    # ---- scan ------------------------------------------------------------------------------

    async def scan(self, persist: bool = True) -> Dict[str, Any]:
        """Scan all in-use taxa, classify, and (optionally) persist actionable rows into
        taxon_synonym_review as pending reviews. Returns stats."""
        cof = await self.load_cof(force=True)
        policy = await self._load_family_policy()
        scan_batch_id = datetime.now().strftime("%Y%m%d-%H%M%S") + "-" + str(uuid.uuid4())[:6]

        inuse = await execute_query(
            'SELECT tt."TaxonID", tt."FullScientificName", f."FamilyName" AS local_family, '
            '       COUNT(*) AS current_dets '
            'FROM "TaxonomicTable" tt '
            'JOIN "Determination" d ON d."TaxonID"=tt."TaxonID" AND d."IsCurrent"=true '
            'LEFT JOIN "Family" f ON f."FamilyID"=tt."FamilyID" '
            'GROUP BY tt."TaxonID", tt."FullScientificName", f."FamilyName"'
        )

        cat_counts: Dict[str, int] = defaultdict(int)
        actionable: List[Dict[str, Any]] = []
        for t in inuse:
            full = t["FullScientificName"] or ""
            c = self.classify(cof, full)
            cat_counts[c["category"]] += 1
            fam_bad, ref_fam = self._family_mismatch(cof, c["genus"], t["local_family"], policy)

            if not self._is_actionable(c, fam_bad):
                continue

            appliable = (c["category"] == EXACT_SYNONYM and bool(c["suggested"]))
            actionable.append({
                "taxon_id": t["TaxonID"],
                "current_full_name": full or f"TaxonID {t['TaxonID']}",
                "current_genus": c["genus"] or None,
                "current_species": c["species"] or None,
                "current_family": t["local_family"],
                "suggested_full_name": c["suggested"] or None,
                "category": c["category"],
                "appliable": appliable,
                "in_use_count": t["current_dets"],
                "candidates": c["candidates"] or None,
                "family_mismatch": fam_bad,
                "reference_family": ref_fam or None,
                "binomial_status": c.get("binomial_status"),
                "scan_batch_id": scan_batch_id,
            })

        stats = {
            "scan_batch_id": scan_batch_id,
            "in_use_taxa": len(inuse),
            "category_counts": dict(cat_counts),
            "actionable": len(actionable),
            "appliable": sum(1 for a in actionable if a["appliable"]),
            "family_mismatch": sum(1 for a in actionable if a["family_mismatch"]),
        }
        if persist:
            inserted = await self._persist(actionable)
            stats["persisted"] = inserted
        return stats

    @staticmethod
    def _is_actionable(c: Dict[str, Any], fam_bad: bool) -> bool:
        """Skip pure EXACT_VALID / NO_BINOMIAL / clean hybrids-trinomials with no issue.
        Surface anything with a name change, a recombination, or a family mismatch."""
        cat = c["category"]
        if fam_bad:
            return True
        if cat in (EXACT_SYNONYM, RECOMBINATION):
            return True
        if cat == TRINOMIAL:
            # only surface subspecies whose binomial part is outdated
            return c.get("binomial_status") in (EXACT_SYNONYM, RECOMBINATION)
        if cat == HYBRID:
            # a hybrid is surfaced only when a parent name looks outdated (has candidates)
            return bool(c.get("candidates"))
        return False  # EXACT_VALID / NO_BINOMIAL / NOT_IN_COF with no family issue

    async def _persist(self, items: List[Dict[str, Any]]) -> int:
        """Replace pending rows and insert the current actionable set."""
        await execute_mutation("DELETE FROM taxon_synonym_review WHERE review_status='pending'")
        n = 0
        for a in items:
            sug = a["suggested_full_name"]
            sug_parts = sug.split() if sug else []
            await execute_mutation(
                """
                INSERT INTO taxon_synonym_review (
                    taxon_id, current_full_name, current_genus, current_species, current_family,
                    suggested_full_name, suggested_genus, suggested_species,
                    issue_type, detection_method, category, appliable, in_use_count,
                    candidates, family_mismatch, reference_family,
                    synonym_type, scan_batch_id, review_status
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,'reference',$10,$11,$12,$13,$14,$15,$16,$17,'pending'
                )
                ON CONFLICT (taxon_id) WHERE review_status='pending'
                DO UPDATE SET
                    current_full_name=EXCLUDED.current_full_name,
                    suggested_full_name=EXCLUDED.suggested_full_name,
                    category=EXCLUDED.category, appliable=EXCLUDED.appliable,
                    in_use_count=EXCLUDED.in_use_count, candidates=EXCLUDED.candidates,
                    family_mismatch=EXCLUDED.family_mismatch,
                    reference_family=EXCLUDED.reference_family,
                    issue_type=EXCLUDED.issue_type, scan_batch_id=EXCLUDED.scan_batch_id,
                    updated_at=CURRENT_TIMESTAMP
                """,
                a["taxon_id"], a["current_full_name"], a["current_genus"], a["current_species"],
                a["current_family"], sug,
                sug_parts[0] if sug_parts else None,
                sug_parts[1] if len(sug_parts) > 1 else None,
                a["category"], a["category"], a["appliable"], a["in_use_count"],
                a["candidates"], a["family_mismatch"], a["reference_family"],
                a["category"], a["scan_batch_id"],
            )
            n += 1
        return n