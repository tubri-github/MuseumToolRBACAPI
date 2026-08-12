"""
Determination write-back for a confirmed taxonomic-name correction.

When the curator confirms "old taxon -> new valid name", every specimen currently determined
as the old taxon gets a NEW current determination pointing at the new taxon, and the old
determination is retired (IsCurrent=false) as history. Modeled on the history-preserving
pattern in _apply_batch_20251119_reclass.py.

Semantics (locked in memory taxon_recheck_rewrite):
  - The new determination is stamped with the CURRENT curator + today (NOT the original
    determiner). Nothing is overwritten -- the original determination survives as an
    IsCurrent=false history row, so a record's full determination history stays intact.
  - Everything runs in one transaction; the apply is recorded in taxon_recheck_apply_log so
    it can be undone in one click.
  - Elasticsearch is reindexed once per affected Primary AFTER the transaction commits.

Determination columns: DeterminerName / Remarks are varchar(50) -> capped. Determiner (int
FK) is left NULL (curator is identified by DeterminerName), matching the migration-import path.
"""
import logging
from datetime import date
from typing import Any, Dict, List, Optional

from app.db.database import get_db, execute_query, execute_single_query, execute_mutation
from app.db.taxon_database import execute_taxon_query, is_taxon_db_configured

logger = logging.getLogger("taxon_apply_service")


def _cap(s: Optional[str], n: int = 50) -> Optional[str]:
    return s[:n] if s else s


async def undo_blocker(apply_log_id: int, inserted_ids: List[int]) -> Optional[str]:
    """Reason this log cannot be undone yet, or None.

    Operations chain through the same determinations, so undos must run newest-first.
    Example: merge A->B inserts row R1 on B; a later merge B->C retires R1 and inserts R2
    on C. Undoing the FIRST one now would delete R1 -- which the second operation's log
    still lists among its retired rows -- and re-flag A's original row as current, leaving
    the specimen with TWO current determinations (one on A, one on C).

    Shared by TaxonApplyService and TaxonMergeService: one apply log, one rule.
    """
    if not inserted_ids:
        return None
    rows = await execute_query(
        'SELECT "DeterminationID", "IsCurrent" FROM "Determination" '
        'WHERE "DeterminationID" = ANY($1::int[])', inserted_ids)
    present = {r["DeterminationID"] for r in rows}
    gone = [i for i in inserted_ids if i not in present]
    retired_since = [r["DeterminationID"] for r in rows if not r["IsCurrent"]]
    affected = gone + retired_since
    if not affected:
        return None

    later = await execute_single_query(
        "SELECT id, operation, applied_at FROM taxon_recheck_apply_log "
        "WHERE id <> $1 AND status = 'applied' AND EXISTS ("
        "  SELECT 1 FROM jsonb_array_elements_text("
        "      coalesce(retired_determination_ids, '[]'::jsonb)) e "
        "  WHERE e::int = ANY($2::int[])) "
        "ORDER BY applied_at DESC LIMIT 1",
        apply_log_id, affected)
    if later:
        return (f"Cannot undo: {len(affected)} of this operation's determinations were "
                f"superseded by a later {later['operation']} (log {later['id']}, "
                f"{later['applied_at']}). Undo that one first.")
    return (f"Cannot undo: {len(affected)} of this operation's determinations are no longer "
            f"current; something changed them afterwards. Undo that change first.")


class TaxonApplyService:

    # ---- target taxon resolution -----------------------------------------------------------

    async def _resolve_family_id(self, conn, genus: str) -> Optional[int]:
        """Best-effort FamilyID for a new taxon: borrow the most common family of existing
        local taxa in the same genus (matched via FullScientificName, since Genus is
        corrupted), else map the CoF family name to a local Family row."""
        rows = await conn.fetch(
            'SELECT "FamilyID", COUNT(*) n FROM "TaxonomicTable" '
            'WHERE "FullScientificName" ILIKE $1 AND "FamilyID" IS NOT NULL '
            'GROUP BY "FamilyID" ORDER BY n DESC LIMIT 1', f"{genus} %")
        if rows:
            return rows[0]["FamilyID"]
        # fall back to CoF genus->family, then local Family by name
        if is_taxon_db_configured():
            try:
                fam = await execute_taxon_query(
                    "SELECT fam.scientific_name AS f FROM taxa g "
                    "JOIN taxa fam ON g.parent_id=fam.id AND fam.rank='FAMILY' "
                    "WHERE g.rank='GENUS' AND g.status='valid' AND lower(g.scientific_name)=lower($1) "
                    "LIMIT 1", genus)
                if fam:
                    lf = await conn.fetch(
                        'SELECT "FamilyID" FROM "Family" WHERE lower("FamilyName")=lower($1) LIMIT 1',
                        fam[0]["f"])
                    if lf:
                        return lf[0]["FamilyID"]
            except Exception as e:  # noqa: BLE001
                logger.warning("CoF family resolve failed for %s: %s", genus, e)
        return None

    async def _find_or_create_target(self, conn, target_name: str,
                                     allow_create: bool = False) -> Dict[str, Any]:
        """Return {taxon_id, created} for an accepted binomial. Matches an existing local row
        by FullScientificName (clean).

        Creating is OFF by default. The family of a taxon invented here can only be guessed
        (most common family among same-genus rows, else the CoF family mapped to a local
        Family row, else NULL) -- and a taxon with no family drops out of family checks,
        statistics and the classification tree without any error. A new name should be created
        deliberately through the taxon-creation form, where the curator sets its family, and
        then applied against that taxon id.
        """
        name = (target_name or "").strip()
        parts = name.split()
        if len(parts) < 2:
            raise ValueError(f"target name is not a binomial: {name!r}")
        # Same tie-break as synonym_service.resolve_to_local_taxon: when the target name
        # exists on more than one local taxon (45 duplicate-FullScientificName groups),
        # attach to the one the collection is already on, not the lowest TaxonID.
        existing = await conn.fetch(
            'SELECT tt."TaxonID" FROM "TaxonomicTable" tt '
            'WHERE lower(TRIM(tt."FullScientificName"))=lower($1) '
            'ORDER BY (SELECT count(*) FROM "Determination" d '
            '          WHERE d."TaxonID" = tt."TaxonID" AND d."IsCurrent" IS TRUE) DESC, '
            '         tt."TaxonID" ASC LIMIT 1', name)
        if existing:
            return {"taxon_id": existing[0]["TaxonID"], "created": False}
        if not allow_create:
            raise ValueError(
                f"'{name}' does not exist in the taxon table. Create it first (so its family "
                f"is set deliberately), then apply against the new taxon.")

        genus, species = parts[0], parts[1]
        family_id = await self._resolve_family_id(conn, genus)
        row = await conn.fetchrow(
            'INSERT INTO "TaxonomicTable" ("Genus","Species","FullScientificName","FamilyID",'
            'created_at, created_via) VALUES ($1,$2,$3,$4,NOW(),\'taxon_recheck\') '
            'RETURNING "TaxonID"', genus, species, name, family_id)
        return {"taxon_id": row["TaxonID"], "created": True}

    @staticmethod
    async def validate_target(target_taxon_id: Optional[int], target_name: Optional[str],
                              allow_create: bool = False) -> Optional[str]:
        """Reason this apply cannot proceed, or None. Called BEFORE anything is started.

        A large apply runs as a background task, so without this the endpoint answers
        "started in background" to a request that is certain to fail, and the curator only
        finds out by opening the apply log.
        """
        if target_taxon_id:
            exists = await execute_single_query(
                'SELECT 1 AS x FROM "TaxonomicTable" WHERE "TaxonID"=$1', target_taxon_id)
            return None if exists else f"target taxon {target_taxon_id} does not exist"
        name = (target_name or "").strip()
        if not name:
            return None  # falls back to the review's own suggestion; checked during apply
        if len(name.split()) < 2:
            return f"'{name}' is not a full name (genus + species)"
        if allow_create:
            return None
        exists = await execute_single_query(
            'SELECT "TaxonID" FROM "TaxonomicTable" '
            'WHERE lower(TRIM("FullScientificName"))=lower($1) LIMIT 1', name)
        if exists:
            return None
        return (f"'{name}' does not exist in the taxon table. Create it first — through the "
                f"taxon form, so its family is set deliberately — then apply against it.")

    # ---- preview (dry-run) -----------------------------------------------------------------

    async def preview(self, review_id: int, target_taxon_id: Optional[int] = None,
                      target_name: Optional[str] = None) -> Dict[str, Any]:
        review = await execute_single_query(
            "SELECT * FROM taxon_synonym_review WHERE id=$1", review_id)
        if not review:
            return {"error": "Review not found"}
        old_taxon_id = review["taxon_id"]
        target_name = target_name or review["suggested_full_name"]

        dets = await execute_query(
            'SELECT COUNT(*) n, COUNT(DISTINCT "PrimaryID") prim '
            'FROM "Determination" WHERE "TaxonID"=$1 AND "IsCurrent"=true', old_taxon_id)
        sample = await execute_query(
            'SELECT "DeterminationID","PrimaryID","Date1","DeterminerName" '
            'FROM "Determination" WHERE "TaxonID"=$1 AND "IsCurrent"=true '
            'ORDER BY "PrimaryID" LIMIT 5', old_taxon_id)

        target = None
        if target_taxon_id:
            tr = await execute_single_query(
                'SELECT "TaxonID","FullScientificName" FROM "TaxonomicTable" WHERE "TaxonID"=$1',
                target_taxon_id)
            target = {"taxon_id": target_taxon_id, "name": tr["FullScientificName"] if tr else None,
                      "exists": bool(tr)}
        elif target_name:
            ex = await execute_query(
                'SELECT "TaxonID" FROM "TaxonomicTable" '
                'WHERE lower(TRIM("FullScientificName"))=lower($1) ORDER BY "TaxonID" LIMIT 1',
                target_name.strip())
            target = {"taxon_id": ex[0]["TaxonID"] if ex else None, "name": target_name.strip(),
                      "exists": bool(ex), "will_create": not ex}

        return {
            "review_id": review_id,
            "old_taxon_id": old_taxon_id,
            "old_name": review["current_full_name"],
            "category": review["category"],
            "appliable": review["appliable"],
            "target": target,
            "current_determinations": dets[0]["n"] if dets else 0,
            "affected_primaries": dets[0]["prim"] if dets else 0,
            "sample": sample,
        }

    # ---- apply -----------------------------------------------------------------------------

    async def _writeback(self, conn, old_taxon_id: int, new_taxon_id: int,
                         target_name: str, who: str, notes) -> Dict[str, Any]:
        """Set-based retire-old + insert-new inside an already-open transaction. Count-
        independent (a handful of statements whether 1 or 10000 determinations)."""
        dets = await conn.fetch(
            'SELECT "DeterminationID","PrimaryID" FROM "Determination" '
            'WHERE "TaxonID"=$1 AND "IsCurrent"=true', old_taxon_id)
        retired_ids = [d["DeterminationID"] for d in dets]
        primary_ids = [d["PrimaryID"] for d in dets]
        inserted_ids: List[int] = []
        if retired_ids:
            await conn.execute(
                'UPDATE "Determination" SET "IsCurrent"=false '
                'WHERE "DeterminationID"=ANY($1::int[])', retired_ids)
            rows = await conn.fetch(
                'INSERT INTO "Determination" '
                '("PrimaryID","TaxonID","IsCurrent","Date1","DeterminerName","Remarks") '
                'SELECT pid, $1, true, $2, $3, $4 FROM unnest($5::int[]) AS pid '
                'RETURNING "DeterminationID"',
                new_taxon_id, date.today(), who, _cap(f"recheck: {target_name}", 50), primary_ids)
            inserted_ids = [r["DeterminationID"] for r in rows]
        return {"retired_ids": retired_ids, "inserted_ids": inserted_ids,
                "primary_ids": primary_ids}

    async def apply(self, review_id: int, applied_by: str,
                    target_taxon_id: Optional[int] = None,
                    target_name: Optional[str] = None,
                    notes: Optional[str] = None,
                    log_id: Optional[int] = None,
                    allow_create: bool = False) -> Dict[str, Any]:
        """Retire old current determinations and insert new ones pointing at the target taxon,
        in one transaction; record to taxon_recheck_apply_log; reindex ES after commit. When
        log_id is given, that pre-created 'running' row is updated instead of inserting a new
        one (background path)."""
        review = await execute_single_query(
            "SELECT * FROM taxon_synonym_review WHERE id=$1", review_id)
        if not review:
            return await self._fail(log_id, "Review not found")
        old_taxon_id = review["taxon_id"]
        old_name = review["current_full_name"]
        target_name = (target_name or review["suggested_full_name"] or "").strip()

        result: Dict[str, Any] = {}
        async with get_db() as conn:
            tx = conn.transaction()
            await tx.start()
            try:
                # 1. resolve/create the target taxon
                if target_taxon_id:
                    tname = await conn.fetchval(
                        'SELECT "FullScientificName" FROM "TaxonomicTable" WHERE "TaxonID"=$1',
                        target_taxon_id)
                    if tname is None:
                        raise ValueError(f"target_taxon_id {target_taxon_id} not found")
                    new_taxon_id, created, target_name = target_taxon_id, False, (tname or target_name)
                else:
                    if not target_name:
                        raise ValueError("no target: pass target_taxon_id or target_name")
                    res = await self._find_or_create_target(conn, target_name, allow_create)
                    new_taxon_id, created = res["taxon_id"], res["created"]
                if new_taxon_id == old_taxon_id:
                    raise ValueError("target equals source taxon; nothing to apply")

                who = _cap(applied_by, 50)
                wb = await self._writeback(conn, old_taxon_id, new_taxon_id, target_name, who, notes)

                # mark the review applied
                await conn.execute(
                    "UPDATE taxon_synonym_review SET review_status='applied', reviewed_by=$1, "
                    "reviewed_at=NOW(), review_notes=$2, final_valid_name=$3, updated_at=NOW() "
                    "WHERE id=$4", who, notes, target_name, review_id)

                # apply-log: insert new, or finalize the pre-created 'running' row
                if log_id is None:
                    log_id = await conn.fetchval(
                        "INSERT INTO taxon_recheck_apply_log "
                        "(review_id, old_taxon_id, new_taxon_id, old_name, new_name, new_taxon_created,"
                        " determinations_changed, retired_determination_ids, inserted_determination_ids,"
                        " affected_primary_ids, applied_by, notes, status) "
                        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'applied') RETURNING id",
                        review_id, old_taxon_id, new_taxon_id, old_name, target_name, created,
                        len(wb["inserted_ids"]), wb["retired_ids"], wb["inserted_ids"],
                        wb["primary_ids"], who, notes)
                else:
                    await conn.execute(
                        "UPDATE taxon_recheck_apply_log SET new_taxon_id=$1, new_name=$2, "
                        "new_taxon_created=$3, determinations_changed=$4, retired_determination_ids=$5, "
                        "inserted_determination_ids=$6, affected_primary_ids=$7, status='applied' "
                        "WHERE id=$8",
                        new_taxon_id, target_name, created, len(wb["inserted_ids"]),
                        wb["retired_ids"], wb["inserted_ids"], wb["primary_ids"], log_id)

                await tx.commit()
                result = {"new_taxon_id": new_taxon_id, "created": created,
                          "inserted_ids": wb["inserted_ids"], "primary_ids": wb["primary_ids"]}
            except Exception as e:  # noqa: BLE001
                await tx.rollback()
                logger.exception("apply failed for review %s", review_id)
                return await self._fail(log_id, f"apply failed: {e}")

        # ES reindex per affected Primary (outside the txn; best-effort)
        reindexed, es_errors = await self._reindex(result["inserted_ids"])
        return {
            "review_id": review_id,
            "apply_log_id": log_id,
            "old_taxon_id": old_taxon_id,
            "new_taxon_id": result["new_taxon_id"],
            "new_taxon_created": result["created"],
            "target_name": target_name,
            "determinations_changed": len(result["inserted_ids"]),
            "affected_primaries": len(set(result["primary_ids"])),
            "es_reindexed": reindexed,
            "es_errors": es_errors,
            "status": "applied",
        }

    async def start_background_apply(self, review_id: int, applied_by: str,
                                     target_taxon_id: Optional[int] = None,
                                     target_name: Optional[str] = None,
                                     notes: Optional[str] = None) -> Dict[str, Any]:
        """Create a 'running' apply-log row and return its id immediately. The caller schedules
        run_background(log_id, ...) as a BackgroundTask. Frontend polls apply-log status."""
        review = await execute_single_query(
            "SELECT taxon_id, current_full_name, suggested_full_name FROM taxon_synonym_review "
            "WHERE id=$1", review_id)
        if not review:
            return {"error": "Review not found"}
        planned = await execute_single_query(
            'SELECT COUNT(*) n FROM "Determination" WHERE "TaxonID"=$1 AND "IsCurrent"=true',
            review["taxon_id"])
        row = await execute_single_query(
            "INSERT INTO taxon_recheck_apply_log "
            "(review_id, old_taxon_id, new_taxon_id, old_name, new_name, total_planned, "
            " applied_by, notes, status) "
            "VALUES ($1,$2,0,$3,$4,$5,$6,$7,'running') RETURNING id",
            review_id, review["taxon_id"], review["current_full_name"],
            (target_name or review["suggested_full_name"]),
            planned["n"] if planned else 0, _cap(applied_by, 50), notes)
        return {"apply_log_id": row["id"], "status": "running",
                "total_planned": planned["n"] if planned else 0}

    async def run_background(self, log_id: int, review_id: int, applied_by: str,
                             target_taxon_id: Optional[int] = None,
                             target_name: Optional[str] = None,
                             notes: Optional[str] = None,
                             allow_create: bool = False) -> None:
        """Background worker: performs the apply into the pre-created 'running' log row."""
        try:
            await self.apply(review_id, applied_by, target_taxon_id, target_name, notes,
                             log_id=log_id, allow_create=allow_create)
        except Exception:  # noqa: BLE001
            logger.exception("run_background apply failed for log %s", log_id)
            await self._fail(log_id, "background apply crashed")

    @staticmethod
    async def _fail(log_id: Optional[int], msg: str) -> Dict[str, Any]:
        if log_id is not None:
            try:
                await execute_mutation(
                    "UPDATE taxon_recheck_apply_log SET status='failed', error_message=$1 WHERE id=$2",
                    msg, log_id)
            except Exception:  # noqa: BLE001
                logger.warning("could not mark apply-log %s failed", log_id)
        return {"error": msg, "apply_log_id": log_id, "status": "failed"}

    @staticmethod
    async def _reindex(determination_ids: List[int]) -> tuple:
        """No-op: Elasticsearch is no longer used in this project.

        Kept so callers keep their (ok, errors) contract. Search runs straight off the
        database now, so applying a determination needs no reindex step.
        """
        return (len(determination_ids), 0)

    # ---- undo ------------------------------------------------------------------------------

    async def undo(self, apply_log_id: int, undone_by: str) -> Dict[str, Any]:
        """Revert an apply: flip retired determinations back to current, delete the inserted
        ones, reindex ES. The created target taxon (if any) is left in place (harmless)."""
        log = await execute_single_query(
            "SELECT * FROM taxon_recheck_apply_log WHERE id=$1", apply_log_id)
        if not log:
            return {"error": "Apply log not found"}
        # The log is shared with TaxonMergeService, but a merge also tags the losing taxon
        # with merged_into_taxon_id; undoing it here would move the determinations back and
        # leave that tag set, i.e. a taxon marked "merged away" that still holds specimens.
        if log["operation"] != "recheck_apply":
            return {"error": f"log {apply_log_id} is a '{log['operation']}', not a recheck "
                             f"apply; undo it from the merge history"}
        if log["status"] == "undone":
            return {"error": "Already undone"}

        retired = log["retired_determination_ids"] or []
        inserted = log["inserted_determination_ids"] or []
        primaries = log["affected_primary_ids"] or []

        blocker = await undo_blocker(apply_log_id, inserted)
        if blocker:
            return {"error": blocker}

        async with get_db() as conn:
            tx = conn.transaction()
            await tx.start()
            try:
                if retired:
                    await conn.execute(
                        'UPDATE "Determination" SET "IsCurrent"=true '
                        'WHERE "DeterminationID"=ANY($1::int[])', retired)
                if inserted:
                    await conn.execute(
                        'DELETE FROM "Determination" WHERE "DeterminationID"=ANY($1::int[])',
                        inserted)
                await conn.execute(
                    "UPDATE taxon_recheck_apply_log SET status='undone', undone_by=$1, "
                    "undone_at=NOW() WHERE id=$2", _cap(undone_by, 50), apply_log_id)
                if log["review_id"]:
                    await conn.execute(
                        "UPDATE taxon_synonym_review SET review_status='pending', "
                        "reviewed_by=NULL, reviewed_at=NULL, final_valid_name=NULL, "
                        "updated_at=NOW() WHERE id=$1", log["review_id"])
                await tx.commit()
            except Exception as e:  # noqa: BLE001
                await tx.rollback()
                logger.exception("undo failed for apply_log %s", apply_log_id)
                return {"error": f"undo failed: {e}"}

        # 以前这里会把受影响的 primary 重新索引到 ES；ES 已停用，搜索直接走数据库，无需此步。
        # 返回值里的 es_* 字段保留，避免调用方/前端因少字段报错。
        return {"apply_log_id": apply_log_id, "status": "undone",
                "determinations_restored": len(retired), "determinations_removed": len(inserted),
                "es_reindexed": len(primaries), "es_errors": 0}