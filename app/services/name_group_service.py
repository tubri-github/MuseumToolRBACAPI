"""Decide one imported name once, instead of once per record.

Why this exists. The importer writes one verbatim_taxonomic row per spreadsheet row and
never deduplicates by name (data_file_processor builds the list inside the row loop), and
apply-suggestion updates a single "PrimaryID". So a batch carrying 1362 rows of
`campostoma anomalum` asks the curator for the same judgement 1362 times. Measured on batch
20251023-001: 26279 species-pending records over 1999 distinct names.

The grouping key is the verbatim name the curator reads on screen, NOT the matched taxon.
Grouping by taxon would be tighter (568 groups instead of 1999 on that batch) but it merges
different source spellings into one decision, so the curator would be approving rows they
never saw. The tighter number is not worth that.

What it writes. The same three things the single-record path writes -- "TaxonID",
"species_verification_status", verification_warnings -- under the same rules: the family
reference check decides verified vs pending, and it is evaluated ONCE per group because it
depends only on the taxon. Nothing here touches the "Primary" table; this is all staging.

Reversibility. One click can rewrite a thousand rows, so every apply records the pre-change
values of every record it touched in batch_name_group_apply.prev_state, and undo replays
them. See migrations/batch_name_group_apply.sql.
"""
import json
import logging
from typing import Any, Dict, List, Optional

from app.db.database import execute_query, execute_single_query, get_db
from app.services.name_decision_service import NameDecisionService
from app.services.taxon_reference_check import (
    MANAGED, build_suggestion_warning, family_reference_warning,
)

logger = logging.getLogger("name_group_service")

# The verbatim name, normalised the same way everywhere: lowercased, collapsed, trimmed.
# Genus and species are separate columns, and either can be blank.
NAME_KEY_SQL = ("btrim(regexp_replace(lower(coalesce(vt.verbatim_genus, '') || ' ' || "
                "coalesce(vt.verbatim_species, '')), '\\s+', ' ', 'g'))")

PENDING_SQL = "coalesce(p.species_verification_status, 'pending') = 'pending'"

# Replaces this module's own warning types and keeps every other warning the record carries;
# an empty result is stored as NULL, which is what the single-record path writes too.
#
# $3 is cast ::text::jsonb and not plain ::jsonb: with a bare jsonb cast asyncpg types the
# parameter as jsonb and encodes the Python string as a jsonb STRING, so an empty "[]" was
# being appended as an element instead of merging to nothing.
_MERGE_WARNINGS_SQL = f"""NULLIF((
    coalesce((
        SELECT jsonb_agg(w) FROM jsonb_array_elements(
            CASE WHEN coalesce(btrim(verification_warnings), '') IN ('', 'null')
                 THEN '[]'::jsonb ELSE verification_warnings::jsonb END) w
        WHERE w->>'issue_type' NOT IN ({', '.join(repr(m) for m in MANAGED)})
    ), '[]'::jsonb) || $3::text::jsonb
)::text, '[]')"""


class NameGroupService:

    # ---- listing -------------------------------------------------------------------------

    @staticmethod
    async def groups(batch_serial_id: str, only_pending: bool = True,
                     min_size: int = 2) -> Dict[str, Any]:
        """Every distinct imported name in the batch, with how many records carry it.

        `suggested_taxon_ids` is deliberately an array: a name whose records point at more
        than one taxon is not a single decision, and the UI has to say so rather than pick
        one. Applying to such a group only touches the taxon the curator confirmed.

        only_pending hides names with nothing left to decide, but it does NOT narrow what is
        counted: every row of the name is aggregated either way, so `verified_records` can
        say how many of it are already settled. That number is what makes an inconsistent
        name (see `state == 'mixed'`) actionable -- the already-verified records were decided
        against one of the conflicting suggestions and need re-checking too, and filtering
        them out of the query is exactly how they would stay invisible.
        """
        # size is judged on the work left when only_pending, on the whole name otherwise
        size_expr = f"count(*) FILTER (WHERE {PENDING_SQL})" if only_pending else "count(*)"
        having = f"{size_expr} >= $2"
        if only_pending:
            having += f" AND count(*) FILTER (WHERE {PENDING_SQL}) > 0"
        rows = await execute_query(
            f"SELECT {NAME_KEY_SQL} AS name_key, "
            "       min(vt.verbatim_genus) AS verbatim_genus, "
            "       min(vt.verbatim_species) AS verbatim_species, "
            "       count(*) AS records, "
            f"      count(*) FILTER (WHERE {PENDING_SQL}) AS pending_records, "
            f"      count(*) FILTER (WHERE NOT ({PENDING_SQL})) AS verified_records, "
            "       array_agg(DISTINCT vt.matched_taxon_id) "
            "         FILTER (WHERE vt.matched_taxon_id IS NOT NULL) AS suggested_taxon_ids, "
            "       array_agg(DISTINCT btrim(vt.verbatim_family)) "
            "         FILTER (WHERE btrim(coalesce(vt.verbatim_family, '')) <> '') "
            "         AS verbatim_families "
            "FROM primary_temp p "
            "JOIN verbatim_taxonomic vt ON vt.verbatim_taxonid = p.verbatim_taxonid "
            f"WHERE p.batch_serial_id = $1 "
            f"  AND {NAME_KEY_SQL} <> '' "
            f"GROUP BY {NAME_KEY_SQL} "
            f"HAVING {having} "
            f"ORDER BY {size_expr} DESC", batch_serial_id, max(1, min_size))

        # Name the suggested taxon for the unambiguous groups -- a bare TaxonID is not
        # something a curator can approve. One query for the whole page, not one per row.
        ids = sorted({t for r in rows for t in (r["suggested_taxon_ids"] or [])})
        names = {}
        if ids:
            for t in await execute_query(
                    'SELECT tt."TaxonID", tt."FullScientificName" AS full_name, '
                    '       f."FamilyName" AS family '
                    'FROM "TaxonomicTable" tt '
                    'LEFT JOIN "Family" f ON f."FamilyID" = tt."FamilyID" '
                    'WHERE tt."TaxonID" = ANY($1::int[])', ids):
                names[t["TaxonID"]] = {"taxon_id": t["TaxonID"], "full_name": t["full_name"],
                                       "family": t["family"]}

        out = []
        for r in rows:
            sug = r["suggested_taxon_ids"] or []
            out.append({
                "name_key": r["name_key"],
                "verbatim_genus": r["verbatim_genus"],
                "verbatim_species": r["verbatim_species"],
                "records": r["records"],
                "pending_records": r["pending_records"],
                "verified_records": r["verified_records"],
                "verbatim_families": r["verbatim_families"] or [],
                "suggestions": [names[t] for t in sug if t in names],
                # one taxon -> one click; none -> nothing to apply; several -> curator picks
                "state": ("single" if len(sug) == 1 else
                          "none" if not sug else "mixed"),
            })

        # A mixed name is not just "two clicks instead of one". The importer answers the same
        # string the same way, so one name pointing at several taxa means the matcher was NOT
        # consistent -- the local taxonomy changed under it (its cache lives an hour), or it
        # hit two duplicate rows of the same name. Whatever the cause, the records it already
        # verified were settled against one of these answers with nobody looking, so they are
        # suspect too. Splitting the counts per taxon is what lets the UI say which side is
        # already committed and how many records that is.
        for g in (g for g in out if g["state"] == "mixed"):
            g["by_taxon"] = await NameGroupService._mixed_breakdown(
                batch_serial_id, g["name_key"], names)

        return {
            "batch_serial_id": batch_serial_id,
            "items": out,
            "total_groups": len(out),
            "total_records": sum(g["records"] for g in out),
            "total_pending": sum(g["pending_records"] for g in out),
            # what the grouping is worth on this batch, so the UI can say it out loud
            "clicks_saved": max(0, sum(g["pending_records"] for g in out) - len(out)),
            # names the matcher was inconsistent about, and how many records it already
            # verified inside them -- the headline number for the warning
            "inconsistent_names": sum(1 for g in out if g["state"] == "mixed"),
            "inconsistent_verified_records": sum(
                g["verified_records"] for g in out if g["state"] == "mixed"),
        }

    @staticmethod
    async def _mixed_breakdown(batch_serial_id: str, name_key: str,
                               names: Dict[int, Dict]) -> List[Dict[str, Any]]:
        """Per-taxon split of one inconsistent name: how many records sit on each answer,
        how many of those are already verified, and record ids to go look at."""
        rows = await execute_query(
            "SELECT vt.matched_taxon_id AS taxon_id, count(*) AS records, "
            f"      count(*) FILTER (WHERE {PENDING_SQL}) AS pending_records, "
            f"      count(*) FILTER (WHERE NOT ({PENDING_SQL})) AS verified_records, "
            "       array_agg(DISTINCT vt.match_status) AS match_statuses, "
            '       (array_agg(p."PrimaryID" ORDER BY p."PrimaryID") '
            f"        FILTER (WHERE NOT ({PENDING_SQL})))[1:5] AS verified_sample_ids "
            "FROM primary_temp p "
            "JOIN verbatim_taxonomic vt ON vt.verbatim_taxonid = p.verbatim_taxonid "
            f"WHERE p.batch_serial_id = $1 AND {NAME_KEY_SQL} = $2 "
            "  AND vt.matched_taxon_id IS NOT NULL "
            "GROUP BY vt.matched_taxon_id "
            "ORDER BY count(*) DESC", batch_serial_id, name_key)
        return [{
            **(names.get(r["taxon_id"]) or {"taxon_id": r["taxon_id"], "full_name": None,
                                            "family": None}),
            "records": r["records"],
            "pending_records": r["pending_records"],
            "verified_records": r["verified_records"],
            # how the importer arrived at this answer -- 'phonetic'/'fuzzy' on one side and
            # 'exact' on the other is the usual shape, and says which side to distrust
            "match_statuses": [s for s in (r["match_statuses"] or []) if s],
            "verified_sample_ids": r["verified_sample_ids"] or [],
        } for r in rows]

    # ---- preview -------------------------------------------------------------------------

    @staticmethod
    async def _affected(batch_serial_id: str, name_key: str, taxon_id: int,
                        whole_name: bool = False,
                        include_ids: Optional[List[int]] = None) -> List[Dict]:
        """The species-pending records this apply would touch.

        By default this is restricted to records whose own suggestion IS the taxon being
        confirmed: the curator clicked Apply on a row showing that suggestion, and in a group
        where the importer suggested several taxa they approved one of them, not the group.

        `whole_name` drops that restriction, for the other way a decision arrives: the curator
        opened the record editor, rejected the suggestion and picked a different taxon. Nothing
        in the batch is matched to what they chose, so the default filter would return zero
        records and the "apply to the other 1357 too" offer would silently do nothing. It also
        happens to be the right repair when the importer answered one spelling inconsistently
        -- one imported name IS one identification, so making them agree is the point.

        `include_ids` forces specific records into the set whatever their status. This is the
        record the curator is deciding in the editor right now: it used to be written by its
        own PUT a second before this call, which left it OUT of the group's prev_state, so
        undo could not restore it -- the group shrank by one record per undo/re-apply cycle
        and the undone answer stayed on that one record. It is passed here INSTEAD of being
        written separately, so its true pre-decision state is what lands in prev_state.
        """
        # $3 is only bound when the clause that uses it is present -- asyncpg rejects a
        # parameter the statement never references.
        params: List[Any] = [batch_serial_id, (name_key or "").strip().lower()]
        selector = PENDING_SQL
        if not whole_name:
            params.append(taxon_id)
            selector = f"vt.matched_taxon_id = $3 AND {selector}"
        ids = [int(i) for i in (include_ids or [])]
        if ids:
            params.append(ids)
            # the forced records bypass BOTH filters: the one being decided may already be
            # verified (the curator is changing their mind) and may carry a different
            # suggestion than the answer they picked
            selector = f'(({selector}) OR p."PrimaryID" = ANY(${len(params)}::int[]))'
        suggestion_clause = ""  # folded into `selector` above
        return await execute_query(
            'SELECT p."PrimaryID", p."TaxonID" AS old_taxon_id, '
            "       coalesce(p.species_verification_status, 'pending') AS old_species, "
            "       coalesce(p.overall_verification_status, 'pending') AS old_overall, "
            "       p.verification_warnings AS old_warnings, "
            "       p.verbatim_taxonid, "
            "       vt.verified_by_name AS old_verified_by, "
            "       vt.verified_at AS old_verified_at, "
            "       vt.verbatim_genus, vt.verbatim_species, "
            "       btrim(coalesce(vt.verbatim_family, '')) AS verbatim_family "
            "FROM primary_temp p "
            "JOIN verbatim_taxonomic vt ON vt.verbatim_taxonid = p.verbatim_taxonid "
            f"WHERE p.batch_serial_id = $1 AND {NAME_KEY_SQL} = $2 "
            f"  {suggestion_clause} AND {selector} "
            'ORDER BY p."PrimaryID"',
            *params)

    async def preview(self, batch_serial_id: str, name_key: str,
                      taxon_id: int, whole_name: bool = False,
                      include_ids: Optional[List[int]] = None) -> Dict[str, Any]:
        """What applying this group would do, before it is done.

        The family reference check is the part worth previewing: when it disagrees the
        records are applied but left PENDING, exactly as the single-record path does. A
        curator who expects 1362 rows to go green deserves to know what will be flagged on
        them.
        """
        taxon = await execute_single_query(
            'SELECT tt."TaxonID", tt."FullScientificName" AS full_name, '
            '       f."FamilyName" AS family '
            'FROM "TaxonomicTable" tt LEFT JOIN "Family" f ON f."FamilyID" = tt."FamilyID" '
            'WHERE tt."TaxonID" = $1', taxon_id)
        if not taxon:
            return {"error": f"taxon {taxon_id} not found"}

        records = await self._affected(batch_serial_id, name_key, taxon_id, whole_name,
                                       include_ids)
        ref_warning = await family_reference_warning(taxon_id)

        # The suggestion warning varies with the record's own imported family, so it is
        # resolved per distinct family in the set -- a handful of values, not a query per row.
        by_family: Dict[str, int] = {}
        for r in records:
            by_family[r["verbatim_family"]] = by_family.get(r["verbatim_family"], 0) + 1
        suggestion_warnings = [
            {"verbatim_family": fam or None, "records": n,
             "warning": build_suggestion_warning(fam, taxon["family"])}
            for fam, n in sorted(by_family.items(), key=lambda kv: -kv[1])]

        return {
            "batch_serial_id": batch_serial_id,
            "name_key": (name_key or "").strip().lower(),
            "taxon": dict(taxon),
            "records_to_apply": len(records),
            "whole_name": whole_name,
            # Always verified, never downgraded -- see the note on apply() below.
            "resulting_status": "verified",
            "family_reference_warning": ref_warning,
            "family_suggestion_warnings": [s for s in suggestion_warnings if s["warning"]],
            "sample_record_ids": [r["PrimaryID"] for r in records[:10]],
        }

    # ---- apply ---------------------------------------------------------------------------

    async def apply(self, batch_serial_id: str, name_key: str, taxon_id: int,
                    applied_by: str, whole_name: bool = False,
                    include_ids: Optional[List[int]] = None) -> Dict[str, Any]:
        pre = await self.preview(batch_serial_id, name_key, taxon_id, whole_name, include_ids)
        if "error" in pre:
            return pre

        records = await self._affected(batch_serial_id, name_key, taxon_id, whole_name,
                                       include_ids)
        if not records:
            return {"error": "nothing to apply: no species-pending record in this batch "
                             + ("carries that name" if whole_name
                                else "carries that name with that suggestion")}

        ref_warning = pre["family_reference_warning"]
        # Verified even when the family disagrees with the reference, and the warning is still
        # written to every record. This deliberately follows the record editor (PUT /records/
        # {id}), which is what the Apply button on the workspace actually calls: a curator
        # confirming a name is trusted, and the family warning is advice they can act on.
        #
        # The alternative -- downgrading to pending, as apply-suggestion and the importer's
        # auto-verify do -- would mean 1362 records approved one at a time all go green while
        # the same 1362 approved by name stay stuck, which reads as the bulk button being
        # broken. It would also strand records the curator has no way to fix yet: correcting a
        # family means moving the taxon, and batch review has no route to that screen.
        #
        # Cost of being wrong here is small in practice: family_reference_policy already
        # absorbs the real disagreements (0 records in batch 20251023-001 carry this warning
        # at all), and the warning stays on the record either way.
        species_status = "verified"
        taxon_family = pre["taxon"]["family"]
        who = (applied_by or "")[:120]

        # Records sharing an imported family get identical warnings, so they can go in one
        # statement each instead of one per record.
        buckets: Dict[str, List[int]] = {}
        for r in records:
            buckets.setdefault(r["verbatim_family"], []).append(r["PrimaryID"])

        prev_state = [{"id": r["PrimaryID"], "t": r["old_taxon_id"], "s": r["old_species"],
                       "o": r["old_overall"], "w": r["old_warnings"],
                       # the verbatim row's stamp, so undo can clear it again
                       "vid": r["verbatim_taxonid"], "vb": r["old_verified_by"],
                       "va": r["old_verified_at"].isoformat()
                             if r["old_verified_at"] else None} for r in records]

        async with get_db() as conn:
            tx = conn.transaction()
            await tx.start()
            try:
                for fam, ids in buckets.items():
                    warnings = [w for w in (ref_warning,
                                            build_suggestion_warning(fam, taxon_family)) if w]
                    await conn.execute(
                        'UPDATE primary_temp SET '
                        '  "TaxonID" = $1, '
                        # cast pinned on both uses: without it Postgres deduces varchar from
                        # the assignment and text from the comparison, and refuses the
                        # parameter as ambiguous
                        '  species_verification_status = $2::varchar, '
                        # same derivation as the single-record editor: locality is not part of
                        # it, because an unmatched locality is allowed to stay NULL
                        "  overall_verification_status = CASE WHEN $2::varchar = 'verified' AND "
                        "     coalesce(record_verification_status, 'pending') = 'verified' "
                        "     THEN 'completed' ELSE 'pending' END, "
                        '  verification_warnings = ' + _MERGE_WARNINGS_SQL + ", "
                        '  "TimeStampModified" = NOW() '
                        'WHERE "PrimaryID" = ANY($4::int[])',
                        taxon_id, species_status, json.dumps(warnings), ids)

                # Stamp who decided. Without it these records are indistinguishable from the
                # 37907 the importer matched by itself: auto_verify_imported_records writes
                # the same "TaxonID" and the same 'verified' status with no human involved.
                # Approving a whole name is still a person deciding, so it is stamped like the
                # single-record path -- what differs is the grain, and the audit row below
                # records that.
                await conn.execute(
                    "UPDATE verbatim_taxonomic SET verified_by_name = $1, verified_at = NOW() "
                    "WHERE verbatim_taxonid = ANY($2::int[])",
                    who or None,
                    [r["verbatim_taxonid"] for r in records if r["verbatim_taxonid"]])

                op_id = await conn.fetchval(
                    "INSERT INTO batch_name_group_apply "
                    "(batch_serial_id, name_key, taxon_id, taxon_name, records_applied, "
                    " species_status, had_family_warning, applied_by, prev_state) "
                    "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb) RETURNING id",
                    batch_serial_id, pre["name_key"], taxon_id, pre["taxon"]["full_name"],
                    len(records), species_status, bool(ref_warning), who,
                    json.dumps(prev_state))
                await tx.commit()
            except Exception as e:  # noqa: BLE001
                await tx.rollback()
                logger.exception("name-group apply failed for %s / %s", batch_serial_id,
                                 name_key)
                return {"error": f"apply failed: {e}"}

        logger.info("name-group apply #%s by %s: batch %s, name '%s' -> taxon %s, "
                    "%s records set to %s%s",
                    op_id, who, batch_serial_id, pre["name_key"], taxon_id, len(records),
                    species_status, " (family reference disagrees)" if ref_warning else "")

        # Remember it for the next batch. This is the cleanest source there is: the curator
        # judged ONE name, so one row is written no matter how many records it settled.
        # Outside the transaction and best-effort -- the apply itself is already committed and
        # must not be rolled back because a reusability record failed.
        await NameDecisionService.record(
            records[0]["verbatim_genus"], records[0]["verbatim_species"], taxon_id,
            decided_by=who, source_batch=batch_serial_id, source="name_group")

        return {
            "op_id": op_id,
            "records_applied": len(records),
            "taxon_id": taxon_id,
            "taxon_name": pre["taxon"]["full_name"],
            "species_status": species_status,
            "family_reference_warning": ref_warning,
            "status": "applied",
        }

    # ---- history / undo ------------------------------------------------------------------

    @staticmethod
    async def history(batch_serial_id: Optional[str] = None,
                      limit: int = 50) -> List[Dict[str, Any]]:
        """Past group applies, newest first. prev_state is omitted -- it can hold thousands
        of rows and nothing in a list view reads it."""
        where = "WHERE batch_serial_id = $2 " if batch_serial_id else ""
        params = [limit] + ([batch_serial_id] if batch_serial_id else [])
        return await execute_query(
            "SELECT id, batch_serial_id, name_key, taxon_id, taxon_name, records_applied, "
            "       species_status, had_family_warning, applied_by, applied_at, status, "
            "       undone_at, undone_by "
            f"FROM batch_name_group_apply {where}"
            "ORDER BY applied_at DESC LIMIT $1", *params)

    @staticmethod
    async def undo_preview(op_id: int) -> Dict[str, Any]:
        """What an undo would and would not restore, before it is done.

        The confirm dialog used to quote the number of records the apply touched, which is not
        the number undo will put back: anything edited since is deliberately skipped, and the
        curator only learned that from the result message afterwards. Now they are told first,
        and which records are being left alone.
        """
        op = await execute_single_query(
            "SELECT * FROM batch_name_group_apply WHERE id = $1", op_id)
        if not op:
            return {"error": "operation not found"}
        if op["status"] == "undone":
            return {"error": "already undone"}

        prev = json.loads(op["prev_state"]) if isinstance(op["prev_state"], str) \
            else op["prev_state"]
        restorable, skipped = await NameGroupService._split_restorable(op, prev)
        return {
            "op_id": op_id,
            "name_key": op["name_key"],
            "taxon_name": op["taxon_name"],
            "records_applied": op["records_applied"],
            "would_restore": len(restorable),
            "would_skip": len(skipped),
            # ids so the curator can go look at what changed instead of being handed a number
            "skipped_record_ids": [s["id"] for s in skipped][:50],
        }

    @staticmethod
    async def _split_restorable(op: Dict, prev: List[Dict]):
        """Records still holding exactly what the apply wrote, versus records touched since.

        A record that no longer matches was decided again by somebody after this apply, and
        this apply is not entitled to revert that -- it may only undo its own writes.
        """
        by_id = {p["id"]: p for p in prev}
        current = {r["PrimaryID"]: r for r in await execute_query(
            'SELECT "PrimaryID", "TaxonID", '
            "       coalesce(species_verification_status, 'pending') AS s "
            'FROM primary_temp WHERE "PrimaryID" = ANY($1::int[])', list(by_id))}
        restorable = [p for p in prev
                      if p["id"] in current
                      and current[p["id"]]["TaxonID"] == op["taxon_id"]
                      and current[p["id"]]["s"] == op["species_status"]]
        restorable_ids = {p["id"] for p in restorable}
        skipped = [p for p in prev if p["id"] not in restorable_ids]
        return restorable, skipped

    @staticmethod
    async def undo(op_id: int, undone_by: str) -> Dict[str, Any]:
        """Put every record this apply touched back the way it was.

        Records edited since are skipped, not overwritten: the apply is not entitled to
        revert a decision someone made after it.
        """
        op = await execute_single_query(
            "SELECT * FROM batch_name_group_apply WHERE id = $1", op_id)
        if not op:
            return {"error": "operation not found"}
        if op["status"] == "undone":
            return {"error": "already undone"}

        prev = json.loads(op["prev_state"]) if isinstance(op["prev_state"], str) \
            else op["prev_state"]
        restorable, skipped_rows = await NameGroupService._split_restorable(op, prev)
        skipped = len(skipped_rows)

        async with get_db() as conn:
            tx = conn.transaction()
            await tx.start()
            try:
                for p in restorable:
                    await conn.execute(
                        'UPDATE primary_temp SET "TaxonID" = $1, '
                        "  species_verification_status = $2, "
                        "  overall_verification_status = $3, "
                        "  verification_warnings = $4, "
                        '  "TimeStampModified" = NOW() '
                        'WHERE "PrimaryID" = $5',
                        p["t"], p["s"], p["o"], p["w"], p["id"])
                    # and take the "a person decided this" stamp back off
                    if p.get("vid"):
                        await conn.execute(
                            "UPDATE verbatim_taxonomic SET verified_by_name = $1, "
                            "verified_at = $2::timestamp WHERE verbatim_taxonid = $3",
                            p.get("vb"), p.get("va"), p["vid"])
                await conn.execute(
                    "UPDATE batch_name_group_apply SET status = 'undone', undone_at = NOW(), "
                    "undone_by = $1 WHERE id = $2", (undone_by or "")[:120], op_id)
                await tx.commit()
            except Exception as e:  # noqa: BLE001
                await tx.rollback()
                logger.exception("undo of name-group apply #%s failed", op_id)
                return {"error": f"undo failed: {e}"}

        logger.info("name-group apply #%s undone by %s: %s records restored, %s skipped "
                    "(edited since)", op_id, undone_by, len(restorable), skipped)
        return {"op_id": op_id, "records_restored": len(restorable),
                "records_skipped": skipped,
                # which ones were left alone, so "5 skipped" is something the curator can act
                # on rather than just a number
                "skipped_record_ids": [s["id"] for s in skipped_rows][:50],
                "status": "undone"}