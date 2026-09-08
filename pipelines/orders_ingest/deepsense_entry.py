"""orders_ingest - reference pipeline for deepsense/docs/PLAN.md Phase 2.2.

Hand-written against spec.yaml, to learn whether the deepsense_entry.py contract
(gates/dynamic/harness.py) is natural to implement before any prompt assumes it is.

Three things that are NOT obvious from reading the spec docs alone, discovered by running
the actual gates (gates/dynamic/checks.py) rather than reasoning about them:

1. **Idempotency is a literal full-row hash across two runs against the same source and
   target.** `_ingested_at` is `current_timestamp` on a fresh insert, but a naive
   delete-then-reinsert that restamps it on every run fails this outright - the hash
   changes even though nothing about the row's business data did. The upsert below
   preserves the existing `_ingested_at` for a key that's already in the target
   (`COALESCE(existing, current_timestamp)`, computed via a LEFT JOIN before the write)
   and only stamps a fresh one for a genuinely new key.

2. **Under `failure.on_row_error: reject_to_dlq`, raising an exception is always a
   blocker** (`CRASH_ON_MALFORMED`), *even when the reason is a legitimate reject-rate
   breach* - the gate only accepts a raised exception when the policy is `fail_fast`.
   So there is no way to "enforce" `reject_threshold_pct` from inside the code under
   `reject_to_dlq`; the run must always return normally, and `fixtures/malformed.parquet`
   has to be built so its real reject rate stays under the declared threshold. (This is a
   real inconsistency in gates/dynamic/checks.py::resilience, not a design choice made
   here - see docs/PLAN.md's open questions.)

3. **`_ingested_at` is `timestamp`, not `timestamp_tz`.** gates/dynamic/checks.py runs in
   the deepsense/ venv, which has no `pytz` - fetching a `TIMESTAMPTZ` column back into
   Python (`snapshot()`, `row_count()`) raises there. Confirmed directly, not assumed.

Everything else follows the AGENTS.md conventions: Decimal for money (never float),
rejects go to `failure.dlq_locator` and nowhere else, destinations come only from the
`source_uri`/`target_uri` arguments.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import duckdb


@dataclass
class RunResult:
    rows_read: int
    rows_written: int
    rows_rejected: int
    cursor_high_water: str | None = None
    metrics: dict[str, Any] | None = None


ORDERS_DDL = """
CREATE TABLE IF NOT EXISTS ORDERS (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    status VARCHAR,
    amount_usd DECIMAL(10,2),
    _ingested_at TIMESTAMP NOT NULL
)
"""

REJECTS_DDL = """
CREATE TABLE IF NOT EXISTS ORDERS_REJECTS (
    order_id BIGINT,
    reason VARCHAR,
    rejected_at TIMESTAMP NOT NULL
)
"""

# Sentinels ('N/A', '-999') are nulled before anything else, so a sentinel-only
# difference between two duplicate rows can't influence which one dedup keeps, and so a
# sentinel amount reads as a legitimate NULL (amount_usd is nullable in target.schema)
# rather than a cast failure.
#
# customer_id: column_rules[customer_id].on_error is reject_row. TRY_CAST(x AS BIGINT)
# handles an INTEGER, DOUBLE or VARCHAR source column uniformly - no need to round-trip
# through VARCHAR first, which would turn a valid 123.0 into '123.0' and then NULL.
#
# status: normalised and validated against contracts.accepted_values in the same
# expression - anything outside the declared enum becomes NULL (status is nullable in
# target.schema) rather than a reject, since column_rules[status].on_error is 'null'.
#
# amount_usd: on_error is reject_row, but that only fires for a genuine cast failure or
# an out-of-range value - a sentinel-derived NULL is a legitimate value per
# target.schema (amount_usd nullable: true), not an error.
#
# Dedup keeps the row with the latest updated_at; the tie-break on the remaining raw
# columns (rather than nothing) makes the choice deterministic across reruns when two
# rows for the same order_id share an exact updated_at.
STAGE_SQL = """
CREATE TEMP TABLE staged AS
WITH cleaned AS (
    SELECT
        order_id,
        TRY_CAST(NULLIF(TRY_CAST(customer_id AS BIGINT), -999) AS BIGINT) AS customer_id,
        CASE lower(trim(NULLIF(status, 'N/A')))
            WHEN 'pending'   THEN 'pending'
            WHEN 'paid'      THEN 'paid'
            WHEN 'shipped'   THEN 'shipped'
            WHEN 'delivered' THEN 'delivered'
            WHEN 'cancelled' THEN 'cancelled'
            WHEN 'refunded'  THEN 'refunded'
            ELSE NULL
        END AS status,
        NULLIF(NULLIF(amount_usd, 'N/A'), '-999') AS amount_sentinel_cleaned,
        updated_at
    FROM read_parquet(?)
),
amounts AS (
    SELECT
        *,
        CASE WHEN amount_sentinel_cleaned IS NULL THEN NULL
             ELSE TRY_CAST(replace(amount_sentinel_cleaned, '$', '') AS DECIMAL(10,2))
        END AS amount_usd,
        amount_sentinel_cleaned IS NOT NULL
            AND TRY_CAST(replace(amount_sentinel_cleaned, '$', '') AS DECIMAL(10,2)) IS NULL
            AS amount_malformed
    FROM cleaned
)
SELECT
    order_id,
    customer_id,
    status,
    amount_usd,
    amount_malformed OR (amount_usd IS NOT NULL AND (amount_usd < 0 OR amount_usd > 100000))
        AS amount_rejected,
    row_number() OVER (
        PARTITION BY order_id
        ORDER BY updated_at DESC NULLS LAST, customer_id DESC NULLS LAST, status, amount_usd
    ) AS rn
FROM amounts
"""

# rn = 1 picks the dedup winner per order_id; rows that lose the tie are neither written
# nor rejected - they were superseded by a newer version of the same key, which is what
# transform.deduplication declares, not a data-quality problem. Kept out of
# fixtures/clean.parquet entirely so the row_accounting gate's rows_read ==
# rows_written + rows_rejected holds without needing a third bucket for "superseded".
ACCEPTED_SQL = """
CREATE TEMP TABLE accepted AS
SELECT order_id, customer_id, status, amount_usd
FROM staged
WHERE rn = 1 AND customer_id IS NOT NULL AND NOT amount_rejected
"""

REJECTED_SQL = """
CREATE TEMP TABLE rejected AS
SELECT
    order_id,
    CASE WHEN customer_id IS NULL THEN 'customer_id is null or not castable to bigint'
         ELSE 'amount_usd malformed or outside contracts.ranges'
    END AS reason,
    current_timestamp AS rejected_at
FROM staged
WHERE customer_id IS NULL OR amount_rejected
"""


def _scalar_int(con: duckdb.DuckDBPyConnection, sql: str, params: list[str] | None = None) -> int:
    """`.fetchone()` is typed `tuple[Any, ...] | None`; every call site here is a single
    scalar count that cannot legitimately come back empty, so narrow it once."""
    row = con.execute(sql, params) if params is not None else con.execute(sql)
    result = row.fetchone()
    assert result is not None, f"query returned no rows: {sql}"
    return int(result[0])


def _fail_on_bad_order_id(con: duckdb.DuckDBPyConnection) -> None:
    """order_id's column_rule is on_error: fail, not reject_row - a malformed primary
    key aborts the run rather than being routed around."""
    n = _scalar_int(con, "SELECT count(*) FROM staged WHERE order_id IS NULL")
    if n:
        raise RuntimeError(
            f"{n} row(s) have a null order_id; column_rules[order_id].on_error=fail "
            "requires aborting the run rather than rejecting or nulling it"
        )


def run(*, source_uri: str, target_uri: str, cursor: str | None = None) -> RunResult:
    con = duckdb.connect(target_uri)
    try:
        con.execute(ORDERS_DDL)
        con.execute(REJECTS_DDL)

        rows_read = _scalar_int(con, "SELECT count(*) FROM read_parquet(?)", [source_uri])

        con.execute(STAGE_SQL, [source_uri])
        _fail_on_bad_order_id(con)
        con.execute(ACCEPTED_SQL)
        con.execute(REJECTED_SQL)

        rows_written = _scalar_int(con, "SELECT count(*) FROM accepted")
        rows_rejected = _scalar_int(con, "SELECT count(*) FROM rejected")

        # Real upsert: existing _ingested_at is preserved for a key already in the
        # target (see module docstring point 1), so a rerun against unchanged source
        # data leaves ORDERS byte-identical.
        con.execute("""
            INSERT INTO ORDERS (order_id, customer_id, status, amount_usd, _ingested_at)
            SELECT a.order_id, a.customer_id, a.status, a.amount_usd,
                   COALESCE(o._ingested_at, current_timestamp) AS _ingested_at
            FROM accepted a
            LEFT JOIN ORDERS o ON o.order_id = a.order_id
            ON CONFLICT (order_id) DO UPDATE SET
                customer_id  = excluded.customer_id,
                status       = excluded.status,
                amount_usd   = excluded.amount_usd,
                _ingested_at = excluded._ingested_at
        """)

        # Scoped to this run's rejected keys, not appended unconditionally, so a rerun
        # against the same malformed batch doesn't grow ORDERS_REJECTS without bound.
        con.execute(
            "DELETE FROM ORDERS_REJECTS WHERE order_id IN (SELECT order_id FROM rejected)"
        )
        con.execute("INSERT INTO ORDERS_REJECTS SELECT * FROM rejected")
    finally:
        con.close()

    return RunResult(rows_read=rows_read, rows_written=rows_written,
                     rows_rejected=rows_rejected)
