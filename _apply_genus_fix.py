"""Repair the corrupted TaxonomicTable."Genus" column (the Cyprinidae genus mess).

Damage: an old import wrote genus+species+family markers into "Genus", e.g.
    Genus = 'Notropislutrensis+[Cyprinidae_SN]![Species] lutren'   (varchar(50), truncated)
"FullScientificName" is intact everywhere (0 blank rows in the whole table), so the
clean genus is recoverable as its first token.

Scope is deliberately split, because "FSN always wins" is NOT safe:

  TIER A  (fixed here)  rows whose Genus carries the import-residue markers
                        ('+[', '_SN]', '![Species]') -> 810 rows, 809 of them Cyprinidae,
                        plus EXTRA_FIX below.
  TIER B  (reported)    the remaining mismatches. For 6 of them the Genus column is
                        the CORRECT one and FullScientificName is the stale/typo'd one
                        (Lepomis vs 'Chaenobryttus gulosus', Elacatinus vs 'Gobiosoma
                        evelynae', Liparis vs 'Liparus sp.', Hemigrammus vs
                        'Hemmigrammus sp.', Aequidens vs 'Aequiden patricki'), and 3 are
                        placeholders/hybrids ('Ameiurus X ?', 'Gobiidae A', 'Not Yet
                        Assigned'). Applying the fix to those would turn right into
                        wrong, so they are only listed for a taxonomic decision.

Duplicates: this only rewrites a column -- no row is merged or deleted, and
"FullScientificName" is never touched, so no duplicate is created. One pre-existing
duplicate becomes visible on (Genus, Species): Yuriria chapalae (TaxonID 12092 + 13009),
whose two rows already carry an identical FullScientificName today. See the separate
duplicate-taxa problem (45 groups) -- NOT handled by this script.

Modes:
  (default)  DRY-RUN  -- counts + full before/after preview written to CSV, no writes.
  --trial    TRIAL    -- executes the UPDATE, verifies, then ROLLS BACK.
  --apply    APPLY    -- writes the undo snapshot first, then executes and COMMITS.

Outputs (always, in every mode):
  genus_fix_preview_tierA.csv   TaxonID, family, FSN, genus_before, genus_after
  genus_fix_review_tierB.csv    the rows a human has to decide on
  genus_fix_undo.sql            restores every tier-A row to its old value

Run: PYTHONIOENCODING=utf-8 python _apply_genus_fix.py [--trial|--apply]
"""
import asyncio
import csv
import io
import sys
from datetime import datetime

from app.db.database import get_db

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

APPLY = "--apply" in sys.argv
TRIAL = "--trial" in sys.argv
WRITE = APPLY or TRIAL

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
PREVIEW_A = "genus_fix_preview_tierA.csv"
REVIEW_B = "genus_fix_review_tierB.csv"
# timestamped: a later dry-run (when tier A is already empty) must never be able to
# overwrite the undo of an applied run.
UNDO_SQL = f"genus_fix_undo_{STAMP}.sql"
# whole-table snapshot, created inside the apply transaction
SNAPSHOT = f"TaxonomicTable_bak_{STAMP}"

# Residue rows whose markers were cut off by the varchar(50) truncation, verified by eye:
#   11972  FSN='Cyprinella lutrensis lutrensis X lutrensis forlonensis'
#          Genus='Cyprinellalutrensis lutrensis X lutrensis forlonen'  -> Cyprinella
EXTRA_FIX = [11972]

# "Genus disagrees with the first token of FullScientificName"
BAD = """(tt."Genus" IS NOT NULL AND btrim(tt."Genus") <> ''
          AND lower(btrim(tt."Genus")) <> lower(split_part(btrim(tt."FullScientificName"), ' ', 1)))"""

# import-residue fingerprint
MARKER = """(tt."Genus" LIKE '%+[%' OR tt."Genus" LIKE '%_SN]%' OR tt."Genus" LIKE '%![Species]%')"""

# the clean genus
FIXED = """split_part(btrim(tt."FullScientificName"), ' ', 1)"""

# safety rails applied on top of tier A membership
SAFE = f"""({FIXED} ~ '^[A-Z][a-z]+$'
            AND NOT EXISTS (SELECT 1 FROM "Family" f
                            WHERE lower(f."FamilyName") = lower({FIXED})))"""

TIER_A = f"({BAD} AND ({MARKER} OR tt.\"TaxonID\" = ANY($1::int[])) AND {SAFE})"
TIER_B = f"({BAD} AND NOT ({MARKER} OR tt.\"TaxonID\" = ANY($1::int[])) )"


def write_csv(path, rows, cols):
    with open(path, "w", encoding="utf-8-sig", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(cols)
        for r in rows:
            w.writerow([r[c] for c in cols])


async def main():
    mode = "APPLY" if APPLY else "TRIAL" if TRIAL else "DRY-RUN"
    print(f"=== mode: {mode}")

    async with get_db() as conn:
        tier_a = await conn.fetch(f"""
            SELECT tt."TaxonID", coalesce(f."FamilyName", '(none)') AS family,
                   tt."FullScientificName" AS fsn,
                   tt."Genus" AS genus_before,
                   {FIXED} AS genus_after,
                   tt."Species"
            FROM "TaxonomicTable" tt
            LEFT JOIN "Family" f ON f."FamilyID" = tt."FamilyID"
            WHERE {TIER_A}
            ORDER BY tt."TaxonID" """, EXTRA_FIX)

        tier_b = await conn.fetch(f"""
            SELECT tt."TaxonID", coalesce(f."FamilyName", '(none)') AS family,
                   tt."FullScientificName" AS fsn,
                   tt."Genus" AS genus_now,
                   {FIXED} AS genus_if_fixed,
                   tt."Species"
            FROM "TaxonomicTable" tt
            LEFT JOIN "Family" f ON f."FamilyID" = tt."FamilyID"
            WHERE {TIER_B}
            ORDER BY tt."TaxonID" """, EXTRA_FIX)

        by_family = await conn.fetch(f"""
            SELECT coalesce(f."FamilyName", '(none)') AS family, count(*) AS n
            FROM "TaxonomicTable" tt
            LEFT JOIN "Family" f ON f."FamilyID" = tt."FamilyID"
            WHERE {TIER_A}
            GROUP BY 1 ORDER BY 2 DESC""", EXTRA_FIX)

        print(f"\ntier A (will be fixed): {len(tier_a)} rows")
        for x in by_family:
            print(f"    {x['family']:<22} {x['n']}")
        print(f"tier B (report only):   {len(tier_b)} rows")

        write_csv(PREVIEW_A, [dict(r) for r in tier_a],
                  ["TaxonID", "family", "fsn", "genus_before", "genus_after", "Species"])
        write_csv(REVIEW_B, [dict(r) for r in tier_b],
                  ["TaxonID", "family", "fsn", "genus_now", "genus_if_fixed", "Species"])
        print(f"\nwrote {PREVIEW_A} ({len(tier_a)}) and {REVIEW_B} ({len(tier_b)})")

        print("\nfirst 10 of tier A:")
        for r in tier_a[:10]:
            print(f"    {r['TaxonID']:<6} {r['fsn']:<34} "
                  f"{str(r['genus_before'])[:36]:<38} -> {r['genus_after']}")

        # undo snapshot -- written in every mode so it is always available
        with open(UNDO_SQL, "w", encoding="utf-8", newline="\n") as fh:
            fh.write("-- undo for _apply_genus_fix.py tier A\n")
            fh.write(f"-- {len(tier_a)} rows, values as they were BEFORE the fix\nBEGIN;\n")
            for r in tier_a:
                old = str(r["genus_before"]).replace("'", "''")
                fh.write(f"UPDATE \"TaxonomicTable\" SET \"Genus\" = '{old}' "
                         f"WHERE \"TaxonID\" = {r['TaxonID']};\n")
            fh.write("COMMIT;\n")
        print(f"wrote {UNDO_SQL}")

        # duplicates the fix makes visible on (Genus, Species)
        dups = await conn.fetch(f"""
            WITH t AS (
              SELECT tt."TaxonID",
                     lower(btrim(coalesce(tt."Genus", ''))) AS g_before,
                     lower(CASE WHEN {TIER_A} THEN {FIXED} ELSE tt."Genus" END) AS g_after,
                     lower(btrim(coalesce(tt."Species", ''))) AS sp
              FROM "TaxonomicTable" tt)
            SELECT g_after, sp, count(*) AS n, count(DISTINCT g_before) AS distinct_before,
                   string_agg("TaxonID"::text, ',' ORDER BY "TaxonID") AS ids
            FROM t GROUP BY 1, 2
            HAVING count(*) > 1 AND count(DISTINCT g_before) > 1""", EXTRA_FIX)
        print(f"\n(Genus,Species) groups that merge because of the fix: {len(dups)}")
        for d in dups:
            print(f"    ({d['g_after']}, {d['sp']}) n={d['n']} TaxonIDs={d['ids']}")

        if not WRITE:
            print("\nDRY-RUN: nothing written to the database.")
            print("Review the two CSVs, then re-run with --trial (executes + rolls back).")
            return

        tr = conn.transaction()
        await tr.start()
        try:
            # full-table snapshot first, inside the same transaction: if the UPDATE
            # commits, the snapshot committed with it; if anything rolls back, neither exists.
            await conn.execute(
                f'CREATE TABLE "{SNAPSHOT}" AS SELECT * FROM "TaxonomicTable"')
            snap_n = await conn.fetchval(f'SELECT count(*) FROM "{SNAPSHOT}"')
            print(f'\nsnapshot table "{SNAPSHOT}" created: {snap_n} rows')

            n = await conn.execute(f"""
                UPDATE "TaxonomicTable" tt SET "Genus" = {FIXED}
                WHERE {TIER_A}""", EXTRA_FIX)
            print(f"\nUPDATE -> {n}")

            left = await conn.fetchval(f"""
                SELECT count(*) FROM "TaxonomicTable" tt WHERE {TIER_A}""", EXTRA_FIX)
            still_bad = await conn.fetchval(f"""
                SELECT count(*) FROM "TaxonomicTable" tt
                WHERE {BAD} AND {MARKER}""")
            fsn_dups = await conn.fetchval("""
                SELECT count(*) FROM (SELECT lower(btrim("FullScientificName"))
                                      FROM "TaxonomicTable" GROUP BY 1
                                      HAVING count(*) > 1) s""")
            print(f"verify: tier-A rows remaining = {left} (expect 0)")
            print(f"verify: rows still carrying residue markers = {still_bad} (expect 0)")
            print(f"verify: FullScientificName duplicate groups = {fsn_dups} (expect 45, unchanged)")

            if left or still_bad:
                raise RuntimeError("verification failed - rolling back")

            if TRIAL:
                await tr.rollback()
                print("\nTRIAL: executed and verified, then ROLLED BACK. Nothing persisted")
                print(f"       (the snapshot table \"{SNAPSHOT}\" was rolled back too).")
            else:
                await tr.commit()
                print("\nAPPLY: committed. Two independent ways back:")
                print(f'   1. table snapshot "{SNAPSHOT}" (whole table, all columns)')
                print(f"   2. {UNDO_SQL} (the 811 Genus values only)")
                print("NEXT: re-run the whole-db taxon check -- Cyprinidae results will change.")
        except Exception:
            if not conn.is_closed():
                try:
                    await tr.rollback()
                except Exception:
                    pass
            raise


asyncio.run(main())
