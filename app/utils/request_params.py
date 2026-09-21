"""Tolerant parsing for values that come straight out of a text field.

Several endpoints take a catalog number, an id list or a year as a path
parameter, and the frontend interpolates whatever the curator typed — including
nothing at all, in which case the URL literally contains "undefined" or "null".
Declaring those parameters as ``int`` makes FastAPI reject the request before the
handler runs, so the page shows a pydantic dump instead of "No result"; parsing
them with a bare ``int()`` is worse, because that surfaces a 500.

The rule these helpers implement:

* a search key that is not a number is not an error, it is a search that matches
  nothing — the caller returns an empty result and the page says "No result";
* a value the caller cannot do anything sensible with (a year that is not a
  year) is a 400 naming the value, never a raw exception.
"""
from datetime import date, datetime
from typing import List, Optional

from fastapi import HTTPException

# "Primary"."CatalogNumber", "Primary"."PrimaryID" and friends are int4
INT4_MIN = -2_147_483_648
INT4_MAX = 2_147_483_647

# t1."Quantity" / "QuantityReturned" / "QuantityResolved" are smallint
SMALLINT_MIN = -32_768
SMALLINT_MAX = 32_767


def parse_int_or_none(raw) -> Optional[int]:
    """An int the DB can actually hold, or None if the text is not one.

    None covers every "this cannot match anything" case at once: blank input,
    the literal strings "undefined"/"null" that a missing frontend variable
    produces, letters, a catalog number with the institution prefix still on it,
    and numbers too large for an int4 column (which would otherwise blow up
    inside asyncpg rather than in Python).
    """
    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        value = raw
    else:
        text = str(raw).strip()
        if not text:
            return None
        try:
            value = int(text)
        except ValueError:
            return None
    return value if INT4_MIN <= value <= INT4_MAX else None


def parse_int_list(raw) -> List[int]:
    """Comma-separated ids, keeping the usable ones and dropping the rest.

    A mixed list still searches on the ids that parsed, so one bad entry does not
    throw away the rest of the query.
    """
    if raw is None:
        return []
    values = []
    for part in str(raw).split(','):
        value = parse_int_or_none(part)
        if value is not None:
            values.append(value)
    return values


def parse_year_start(raw, field: str = "year") -> date:
    """January 1st of ``raw``, or a 400 that names what was wrong.

    Used by the /…count/{year} endpoints, which previously fed the value to
    ``datetime.strptime`` unguarded and answered a 500.
    """
    text = str(raw).strip() if raw is not None else ""
    try:
        return datetime.strptime(f"{text}-01-01", "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid {field}: {text!r}. Expected a four-digit year, for example 2026.",
        )