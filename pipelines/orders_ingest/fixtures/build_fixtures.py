"""Synthesizes fixtures/clean.parquet and fixtures/malformed.parquet.

Per AGENTS.md: fixtures are synthesized from the schema, never sampled from a live
source. Run once from this directory (`python build_fixtures.py`) to regenerate; the
generated .parquet files are committed, this script is not consumed by the gates.

clean.parquet: 26 rows, no duplicates, no rejects - exercises
gates/dynamic/checks.py::row_accounting and ::idempotency, both of which require
rows_read == rows_written (idempotency additionally requires a byte-identical rerun).
Sentinel values ('N/A', '-999') are still included here deliberately: they must resolve
to a legitimate NULL, not a reject, so belong in the fixture the gates count on being
reject-free.

malformed.parquet: 31 rows (26 base + 1 duplicate + 4 rejects), reject rate held under
spec.yaml's failure.reject_threshold_pct (20%; actual is 4/31 = 12.9%) - see
deepsense_entry.py's module docstring for why exceeding it can't be enforced by raising.
One duplicate order_id is included to exercise dedup outside of the
row-accounting-checked fixture.
"""
from __future__ import annotations

from pathlib import Path

import duckdb

HERE = Path(__file__).parent


def build_clean() -> None:
    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE TABLE orders (
            order_id BIGINT, customer_id BIGINT, status VARCHAR,
            amount_usd VARCHAR, updated_at TIMESTAMP
        )
    """)
    rows = []
    statuses = ["pending", "paid", "shipped", "delivered", "cancelled", "refunded"]
    for i in range(1, 21):
        amount = f"${i * 12}.50" if i % 3 == 0 else f"{i * 12}.50"
        rows.append((i, 1000 + i, statuses[i % len(statuses)], amount,
                     f"2026-01-{(i % 28) + 1:02d} 00:00:00"))
    # Sentinel values must land here: they resolve to a legitimate NULL amount_usd
    # (nullable in target.schema), which is an accept, not a reject.
    rows += [
        (21, 1021, "paid", "N/A", "2026-01-21 00:00:00"),
        (22, 1022, "paid", "-999", "2026-01-22 00:00:00"),
    ]
    # A status outside contracts.accepted_values resolves to NULL (status is nullable),
    # per column_rules[status].on_error: 'null' - also not a reject.
    rows += [
        (23, 1023, "UNKNOWN_LEGACY_STATUS", "45.00", "2026-01-23 00:00:00"),
        (24, 1024, "  Paid  ", "12.00", "2026-01-24 00:00:00"),
    ]
    rows += [
        (25, 1025, "shipped", "0.00", "2026-01-25 00:00:00"),        # boundary: min
        (26, 1026, "delivered", "100000.00", "2026-01-26 00:00:00"),  # boundary: max
    ]
    con.executemany("INSERT INTO orders VALUES (?,?,?,?,?)", rows)
    con.execute(f"COPY orders TO '{HERE / 'clean.parquet'}' (FORMAT parquet)")
    con.close()
    print(f"wrote {len(rows)} rows -> clean.parquet")


def build_malformed() -> None:
    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE TABLE orders (
            order_id BIGINT, customer_id BIGINT, status VARCHAR,
            amount_usd VARCHAR, updated_at TIMESTAMP
        )
    """)
    rows: list[tuple[int, int | None, str, str, str]] = []
    statuses = ["pending", "paid", "shipped", "delivered", "cancelled", "refunded"]
    for i in range(101, 127):
        rows.append((i, 2000 + i, statuses[i % len(statuses)], f"{i}.00",
                     f"2026-02-{(i % 28) + 1:02d} 00:00:00"))
    # A later duplicate for order 101 - dedup must keep this one (newer updated_at).
    rows.append((101, 9999, "paid", "999.00", "2026-03-01 00:00:00"))

    # Four genuine rejects (13.3% of 30 rows, under the 20% threshold):
    rows += [
        (201, None, "paid", "50.00", "2026-02-01 00:00:00"),            # null customer_id
        (202, 2202, "paid", "not-a-number", "2026-02-02 00:00:00"),      # uncastable
        (203, 2203, "paid", "-50.00", "2026-02-03 00:00:00"),            # below ranges.min
        (204, 2204, "paid", "250000.00", "2026-02-04 00:00:00"),         # above ranges.max
    ]
    con.executemany("INSERT INTO orders VALUES (?,?,?,?,?)", rows)
    con.execute(f"COPY orders TO '{HERE / 'malformed.parquet'}' (FORMAT parquet)")
    con.close()
    print(f"wrote {len(rows)} rows -> malformed.parquet (4 expected rejects, "
          f"{4 / len(rows) * 100:.1f}%)")


if __name__ == "__main__":
    build_clean()
    build_malformed()
