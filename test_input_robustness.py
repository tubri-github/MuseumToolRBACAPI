#!/usr/bin/env python
"""
Bad-input regression tests: a typed field must never surface a raw 422/500.

Everything here came out of the 2026-08-19 sweep of the add-loan form and the
other pages that interpolate a typed value straight into a URL. The rule the
tests encode:

  * a value that is not a number  -> an empty result (the page already says
    "No result"), never a validation dump;
  * a year that is not a year, or a quantity/loan number the DB will reject ->
    400 with a sentence the curator can act on, never a Postgres error string;
  * nothing that fails may leave a partial row behind.

No loan is created: both create-path cases are rejected before the stored
procedure runs (and the procedure is a single statement, so a failure inside it
rolls itself back anyway).

Standalone (no pytest needed):  PYTHONIOENCODING=utf-8 python test_input_robustness.py
"""
import asyncio
import sys

import httpx

from main import app
from app.db.database import execute_query

# what a curator can actually get into one of these fields
JUNK = ["abc", "TU%201234", "12345A", "undefined", "null", "123.5"]

# a real catalog number, filled in by main() so the happy path is covered too
KNOWN_CATALOG = None


def client():
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


async def _get(url):
    async with client() as c:
        return await c.get(url)


# --- catalog number typed into the loan form -----------------------------

async def test_lotstring_treats_a_non_number_as_no_result():
    for v in JUNK:
        r = await _get(f"/api/lots/lotString/{v}")
        assert r.status_code == 200, f"lotString/{v} -> {r.status_code} {r.text[:160]}"
        assert r.json()["data"]["total"] == 0, f"lotString/{v} unexpectedly matched something"


async def test_lotstring_survives_a_number_too_big_for_the_column():
    r = await _get("/api/lots/lotString/99999999999")
    assert r.status_code == 200, f"out-of-range catalog -> {r.status_code} {r.text[:160]}"
    assert r.json()["data"]["total"] == 0


async def test_lotstring_still_finds_a_real_catalog_number():
    assert KNOWN_CATALOG is not None, "no catalog number available in this database"
    r = await _get(f"/api/lots/lotString/{KNOWN_CATALOG}")
    assert r.status_code == 200, f"lotString/{KNOWN_CATALOG} -> {r.status_code}"
    assert r.json()["data"]["total"] >= 1, f"catalog {KNOWN_CATALOG} should exist"


async def test_lotstring_tolerates_padding_around_the_number():
    assert KNOWN_CATALOG is not None
    r = await _get(f"/api/lots/lotString/%20{KNOWN_CATALOG}%20")
    assert r.status_code == 200, f"padded catalog -> {r.status_code} {r.text[:160]}"
    assert r.json()["data"]["total"] >= 1, "surrounding spaces should not lose the match"


# --- deaccession page: /lot/{ids}/{limit} --------------------------------

async def test_lot_by_ids_treats_junk_as_no_result():
    """views/Lot/deaccesion.vue interpolates its catalog input here; the logs
    already show six /api/lots/lot/null/1 hits that 500'd."""
    for v in ["null", "abc", "undefined"]:
        r = await _get(f"/api/lots/lot/{v}/1")
        assert r.status_code == 200, f"lot/{v}/1 -> {r.status_code} {r.text[:160]}"
        assert r.json()["data"]["total"] == 0, f"lot/{v}/1 unexpectedly matched something"


async def test_lot_by_ids_keeps_the_good_ids_in_a_mixed_list():
    assert KNOWN_CATALOG is not None
    r = await _get(f"/api/lots/lot/{KNOWN_CATALOG},abc/1")
    assert r.status_code == 200, f"mixed id list -> {r.status_code} {r.text[:160]}"
    assert r.json()["data"]["total"] >= 1, "the valid id in the list should still match"


# --- year counters -------------------------------------------------------

async def test_year_counters_reject_a_non_year_with_a_readable_message():
    for path in ("/api/lots/lotcount/{}", "/api/loan/loancount/{}", "/api/locality/localitycount/{}"):
        r = await _get(path.format("abc"))
        assert r.status_code == 400, f"{path.format('abc')} -> {r.status_code} {r.text[:160]}"
        detail = r.json()["detail"]
        assert "abc" in detail and "year" in detail.lower(), f"unhelpful message: {detail!r}"


async def test_year_counters_still_work_for_a_real_year():
    for path in ("/api/lots/lotcount/{}", "/api/loan/loancount/{}", "/api/locality/localitycount/{}"):
        r = await _get(path.format("2025"))
        assert r.status_code == 200, f"{path.format('2025')} -> {r.status_code} {r.text[:160]}"
        assert "total" in r.json()["data"]


# --- the shared parser ---------------------------------------------------

async def test_id_list_parser_keeps_the_usable_entries():
    """/api/search/lots and /loans parse their ids with this too, but those two
    endpoints cannot be exercised end to end: Elasticsearch is retired, so they
    fail on `es.search` regardless of input. They have no frontend caller."""
    from app.utils.request_params import parse_int_list, parse_int_or_none

    assert parse_int_list("1,2,3") == [1, 2, 3]
    assert parse_int_list("1,abc,3") == [1, 3], "one bad entry must not discard the rest"
    assert parse_int_list("abc") == []
    assert parse_int_list("") == []
    assert parse_int_list(None) == []
    assert parse_int_list(" 7 , 8 ") == [7, 8]
    assert parse_int_list("1,99999999999") == [1], "out-of-range ids must be dropped, not raised"

    assert parse_int_or_none("42") == 42
    assert parse_int_or_none(" 42 ") == 42
    assert parse_int_or_none("undefined") is None
    assert parse_int_or_none("null") is None
    assert parse_int_or_none("12345A") is None
    assert parse_int_or_none("123.5") is None
    assert parse_int_or_none("99999999999") is None
    assert parse_int_or_none(True) is None, "a bool must not sneak through as 1"


# --- add loan: the two DB-level errors that reached the curator raw ------

async def test_duplicate_loan_number_is_rejected_with_the_number_in_the_message():
    existing = await execute_query('SELECT "LoanNumber" FROM t2 WHERE "LoanNumber" IS NOT NULL LIMIT 1')
    number = existing[0]["LoanNumber"]
    async with client() as c:
        r = await c.post("/api/loan/loan", json={
            "loanNumber": number, "transactionType": "Loan", "loanDetails": []})
    assert r.status_code == 400, f"duplicate loan number -> {r.status_code} {r.text[:200]}"
    assert number in r.json()["detail"], f"message does not name the number: {r.json()['detail']!r}"

    # and nothing was created
    rows = await execute_query('SELECT count(*) AS c FROM t2 WHERE "LoanNumber" = $1', number)
    assert rows[0]["c"] == 1, f"{rows[0]['c']} rows now hold loan number {number}"


async def test_quantity_beyond_the_column_limit_is_rejected_before_the_procedure():
    """t1.Quantity/QuantityReturned/QuantityResolved are smallint (max 32767)."""
    async with client() as c:
        r = await c.post("/api/loan/loan", json={
            "loanNumber": "ZZ-AUTOTEST-QTY", "transactionType": "Loan",
            "loanDetails": [{"PrimaryID": "629041", "Quantity": 999999}]})
    assert r.status_code == 400, f"oversized quantity -> {r.status_code} {r.text[:200]}"
    detail = r.json()["detail"]
    assert "32767" in detail, f"message does not state the limit: {detail!r}"

    rows = await execute_query('SELECT count(*) AS c FROM t2 WHERE "LoanNumber" = $1', "ZZ-AUTOTEST-QTY")
    assert rows[0]["c"] == 0, "a loan was created despite the rejected quantity"


# --- runner ---------------------------------------------------------------

TESTS = [
    "test_lotstring_treats_a_non_number_as_no_result",
    "test_lotstring_survives_a_number_too_big_for_the_column",
    "test_lotstring_still_finds_a_real_catalog_number",
    "test_lotstring_tolerates_padding_around_the_number",
    "test_lot_by_ids_treats_junk_as_no_result",
    "test_lot_by_ids_keeps_the_good_ids_in_a_mixed_list",
    "test_year_counters_reject_a_non_year_with_a_readable_message",
    "test_year_counters_still_work_for_a_real_year",
    "test_id_list_parser_keeps_the_usable_entries",
    "test_duplicate_loan_number_is_rejected_with_the_number_in_the_message",
    "test_quantity_beyond_the_column_limit_is_rejected_before_the_procedure",
]


async def main():
    global KNOWN_CATALOG
    rows = await execute_query(
        'SELECT "CatalogNumber" FROM "Primary" WHERE "CatalogNumber" IS NOT NULL '
        'AND "CatalogNumber" > 0 ORDER BY "CatalogNumber" LIMIT 1')
    KNOWN_CATALOG = rows[0]["CatalogNumber"] if rows else None

    failures = []
    for name in TESTS:
        try:
            await globals()[name]()
            print(f"PASS  {name}")
        except Exception as e:
            failures.append((name, e))
            print(f"FAIL  {name}")
            print(f"      {type(e).__name__}: {e}")

    print(f"\n{len(TESTS) - len(failures)}/{len(TESTS)} passed")
    if failures:
        print("\nfailing:")
        for name, e in failures:
            print(f"  - {name}: {type(e).__name__}: {e}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))