"""One test per spec.yaml contracts: entry, named so
deepsense/gates/contracts/coverage.py can find it by prefix (test_contract_<key>*).

Runs deepsense_entry.run() against fixtures/clean.parquet into a fresh per-test target
(pytest's tmp_path), then asserts the declared guarantee against the target state - never
against the implementation's own RunResult claims alone, so a pipeline that lies about
what it wrote is still caught.

Each test states, in its own docstring, what an implementation would have to do wrong to
fail it - the falsifiability check every test here is meant to satisfy.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import duckdb

_ENTRY_PATH = Path(__file__).resolve().parents[1] / "deepsense_entry.py"
_CLEAN_FIXTURE = str(Path(__file__).resolve().parents[1] / "fixtures" / "clean.parquet")


def _scalar_int(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    """`.fetchone()` is typed `tuple[Any, ...] | None`; every call site here is a single
    scalar count that cannot legitimately come back empty, so narrow it once."""
    result = con.execute(sql).fetchone()
    assert result is not None, f"query returned no rows: {sql}"
    return int(result[0])


def _load_entry() -> ModuleType:
    spec = importlib.util.spec_from_file_location("deepsense_entry", _ENTRY_PATH)
    assert spec is not None and spec.loader is not None, f"could not load {_ENTRY_PATH}"
    mod = importlib.util.module_from_spec(spec)
    # See deepsense/gates/dynamic/harness.py::load_entrypoint for why this line is
    # required: deepsense_entry.py uses `from __future__ import annotations` on a
    # dataclass, and dataclasses resolves string annotations via
    # sys.modules[cls.__module__], which doesn't exist until this is set.
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _run_clean(tmp_path: Path) -> tuple[str, Any]:
    entry = _load_entry()
    target = str(tmp_path / "target.duckdb")
    result = entry.run(source_uri=_CLEAN_FIXTURE, target_uri=target)
    return target, result


def test_contract_row_count(tmp_path: Path) -> None:
    """contracts.row_count: {min: 1}.

    Fails if the pipeline writes nothing at all - e.g. every rule silently mapped to
    on_error: 'null' and the WHERE clause upstream excluded every row.
    """
    target, _ = _run_clean(tmp_path)
    con = duckdb.connect(target, read_only=True)
    try:
        n = _scalar_int(con, "SELECT count(*) FROM ORDERS")
    finally:
        con.close()
    assert n >= 1, f"expected at least 1 row in ORDERS, found {n}"


def test_contract_uniqueness(tmp_path: Path) -> None:
    """contracts.uniqueness: [[order_id]].

    Fails if dedup is missing or broken - e.g. QUALIFY/row_number partitioned on the
    wrong column, or a naive INSERT with no ON CONFLICT handling for a rerun.
    """
    target, _ = _run_clean(tmp_path)
    con = duckdb.connect(target, read_only=True)
    try:
        total = _scalar_int(con, "SELECT count(*) FROM ORDERS")
        distinct = _scalar_int(con, "SELECT count(DISTINCT order_id) FROM ORDERS")
    finally:
        con.close()
    assert total == distinct, f"{total} rows but only {distinct} distinct order_id values"


def test_contract_not_null(tmp_path: Path) -> None:
    """contracts.not_null: [order_id, customer_id, _ingested_at].

    Fails if a reject path leaks a null business key into the target instead of routing
    the row to the DLQ - the exact failure class the Reviewer eval planted (see
    deepsense/evals/reviewer_variants.py) and looked for by reading the code, not by
    running this test. This test is the black-box half of that same coverage.
    """
    target, _ = _run_clean(tmp_path)
    con = duckdb.connect(target, read_only=True)
    try:
        for col in ("order_id", "customer_id", "_ingested_at"):
            n = _scalar_int(con, f"SELECT count(*) FROM ORDERS WHERE {col} IS NULL")
            assert n == 0, f"{n} row(s) have a null {col}"
    finally:
        con.close()


def test_contract_accepted_values(tmp_path: Path) -> None:
    """contracts.accepted_values: status in the declared enum (or NULL).

    Fails if status normalisation is missing - e.g. a raw 'Paid' or trailing-whitespace
    value from fixtures/clean.parquet survives uncleaned, or an out-of-domain legacy
    value ('UNKNOWN_LEGACY_STATUS', present in the fixture) is written verbatim instead
    of nulled.
    """
    allowed = {"pending", "paid", "shipped", "delivered", "cancelled", "refunded", None}
    target, _ = _run_clean(tmp_path)
    con = duckdb.connect(target, read_only=True)
    try:
        values = {r[0] for r in con.execute("SELECT DISTINCT status FROM ORDERS").fetchall()}
    finally:
        con.close()
    assert values <= allowed, f"status contains values outside the contract: {values - allowed}"
    # The fixture specifically includes 'UNKNOWN_LEGACY_STATUS' and '  Paid  ' to prove
    # normalisation actually ran, not merely that the source happened to be clean.
    assert None in values, (
        "expected at least one NULL status (fixture includes an out-of-domain legacy "
        "value that must normalise to NULL, not survive as-is)"
    )


def test_contract_ranges(tmp_path: Path) -> None:
    """contracts.ranges: amount_usd in [0.00, 100000.00].

    Fails if the range check is only asserted and never enforced - e.g. an
    out-of-range value reaches the target because on_error: reject_row for amount_usd
    was implemented for cast failures but not for in-range-but-out-of-contract values.
    The fixture includes exact boundary values (0.00 and 100000.00, both must be kept)
    so this test cannot pass by accident on a rule that rejects everything.
    """
    target, _ = _run_clean(tmp_path)
    con = duckdb.connect(target, read_only=True)
    try:
        out_of_range = _scalar_int(
            con, "SELECT count(*) FROM ORDERS WHERE amount_usd < 0.00 OR amount_usd > 100000.00")
        boundaries = _scalar_int(
            con, "SELECT count(*) FROM ORDERS WHERE amount_usd IN (0.00, 100000.00)")
    finally:
        con.close()
    assert out_of_range == 0, f"{out_of_range} row(s) violate contracts.ranges.amount_usd"
    assert boundaries == 2, (
        f"expected both boundary values (0.00, 100000.00) from the fixture to survive, "
        f"found {boundaries}"
    )


def test_contract_rejects(tmp_path: Path) -> None:
    """contracts.rejects: {max_pct: 20.0}, exercised against fixtures/malformed.parquet.

    Fails if rejected rows are dropped without a trace (see
    deepsense/evals/reviewer_variants.py's planted defect) or if the reject rate exceeds
    the declared threshold. Deliberately does NOT assert that run() raises when the
    threshold is exceeded: under failure.on_row_error: reject_to_dlq,
    gates/dynamic/checks.py::resilience treats any raised exception as CRASH_ON_MALFORMED
    regardless of cause, so a compliant implementation must always return normally and
    let the reject-rate number speak for itself.
    """
    entry = _load_entry()
    target = str(tmp_path / "target.duckdb")
    malformed = str(Path(__file__).resolve().parents[1] / "fixtures" / "malformed.parquet")
    result = entry.run(source_uri=malformed, target_uri=target)

    assert result.rows_rejected > 0, "fixture includes malformed rows; expected some rejects"
    reject_pct = result.rows_rejected / result.rows_read * 100
    assert reject_pct <= 20.0, f"reject rate {reject_pct:.1f}% exceeds contracts.rejects.max_pct"

    con = duckdb.connect(target, read_only=True)
    try:
        dlq_count = _scalar_int(con, "SELECT count(*) FROM ORDERS_REJECTS")
    finally:
        con.close()
    assert dlq_count == result.rows_rejected, (
        f"RunResult claims {result.rows_rejected} rejected but ORDERS_REJECTS has "
        f"{dlq_count} rows - rejects were counted but not all written to the DLQ"
    )
