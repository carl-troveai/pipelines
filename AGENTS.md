# House conventions

Read by every agent working in this repo — the Engineer reads this before writing a line
of pipeline code, and the Reviewer reads it to know what "conforms to house style" means
beyond the spec. These are the things a spec shouldn't have to repeat on every pipeline.

Everything here is enforced by `deepsense/gates/`, not by good intentions. A rule with no
gate behind it doesn't belong in this file — file it in `deepsense/gates/static/patterns.yaml`
or `deepsense/gates/dynamic/checks.py` instead.

## Directory layout

One pipeline, one directory, directly under `pipelines/`:

```
pipelines/<name>/
    ADR.md              why this stack was chosen - prose, cites profile.json numbers
    spec.yaml           the contract - source, target, transform, contracts:
    profile.json        what the data actually looks like (source pipelines only)
    deepsense_entry.py  required uniform entrypoint, see below
    <implementation>    whatever the ADR chose - a module, a dbt project, a Spark job
    tests/              one test per spec.yaml `contracts:` entry
    fixtures/
        clean.parquet       well-formed rows exercising every declared contract
        malformed.parquet   sentinel nulls, wrong types, dupes, out-of-range - synthesized
                            from the schema, never sampled from a live source
```

**Naming:** `<name>` is lowercase `snake_case`, one path segment directly under
`pipelines/` (`orders_ingest`, not `Orders-Ingest` or `ingest/orders`). CI resolves the
touched pipeline by splitting a changed path on `/` and taking the second segment
(`pipelines/<name>/...`) — a nested or oddly-cased name breaks that resolution, not just
the style guide.

## The one contract every stack must expose

```python
# pipelines/<name>/deepsense_entry.py
def run(*, source_uri: str, target_uri: str, cursor: str | None = None) -> RunResult: ...
```

`RunResult` carries `rows_read`, `rows_written`, `rows_rejected`, `cursor_high_water`,
`metrics`. This is the only thing `deepsense/gates/dynamic/harness.py` knows about your
implementation — it never inspects the stack directly. For dbt, shell out to `dbt run`.
For Polars, call the module. For Spark, submit the job.

## Non-negotiables

1. **Idempotency.** Running twice leaves the target identical. `write_mode: upsert` means
   a real update-on-conflict — never `INSERT ... ON CONFLICT DO NOTHING`, which silently
   drops every update and still passes a first-run test. Caught by `NOT_IDEMPOTENT`.
2. **Row accounting.** `rows_read == rows_written + rows_rejected`, always. A row that is
   filtered, deduplicated away, or dropped without being counted somewhere is the failure
   that survives longest in production, because nothing errors and the numbers look
   plausible. Caught by `ROWS_UNACCOUNTED`.
3. **Rejects go to the DLQ named in `spec.failure.dlq_locator`.** Not a log line, not a
   `print()`, not silently discarded. A pipeline can compute `rows_rejected` correctly and
   still fail this if the rows never land anywhere — the count alone doesn't prove they
   were written; verified by reading the code, not by re-deriving the number.
4. **Destinations come from `source_uri`/`target_uri`, never hardcoded.** The gates run
   your pipeline twice against two different targets. Caught by `SINK_NOT_HONORED` and the
   static `HARDCODED_HOST` rule.
5. **Decimal for money.** Never `float` for an amount, price, balance, or revenue column.
   Caught by `FLOAT_MONEY`.
6. **Timezone-aware timestamps.** `datetime.now(timezone.utc)`, never naive. Caught by
   `NAIVE_DATETIME_NOW`.
7. **Only declared columns reach the target.** An undeclared output column fails
   `UNDECLARED_COLUMN` and breaks schema-drift detection for every pipeline downstream of
   this one's target.

## Audit column

Every pipeline's target schema includes:

```yaml
- {name: _ingested_at, type: timestamp, nullable: false}
```

populated via a column rule with `source: current_timestamp`. It is a per-run audit
timestamp, not business data — it is *supposed* to change on every ingest of the same key.
If `spec.semantics.idempotency_guarantee` is declared, say explicitly that it is scoped to
business columns and excludes `_ingested_at`; a blanket "rerun produces an identical
target" claim will be read literally by a dynamic idempotency check and by the Reviewer.

## Logging

Use the configured logger. `print()` is flagged by `PRINT_DEBUGGING` (minor, but a fix
iteration is a fix iteration). No logger name or format is mandated yet beyond that — this
file is where that convention will get pinned down once a second pipeline exists to check
it against.

## Forbidden patterns

The full, authoritative list is `deepsense/gates/static/patterns.yaml` (14 rules, regex
over source text, deliberately dumb). The ones worth knowing before you write anything:

| Rule | Severity | Why |
|---|---|---|
| `SILENT_EXCEPTION` | blocker | Swallowed exception. Rows fail loudly or route to the DLQ - never vanish into a `pass`. |
| `DESTRUCTIVE_SQL_UNSCOPED` | blocker | No `DROP`/`TRUNCATE`. Full refresh is `CREATE OR REPLACE`. |
| `DELETE_WITHOUT_WHERE` | blocker | An unscoped `DELETE` is a full-clear happening by accident. |
| `HARDCODED_HOST` / `HARDCODED_CREDENTIAL` | blocker | Connections come from `spec.connection.secret_ref`, never a literal. |
| `UNBOUNDED_READ` | major | `SELECT *` with no `WHERE`/`LIMIT`. Incremental reads filter on `spec.source.read.cursor_column`. |
| `BARE_EXCEPT` | major | Catches `KeyboardInterrupt`/`SystemExit` along with the error you meant to handle. |

Suppress a real false positive with `# deepsense: allow RULE_ID — reason`, visible in the
diff. An unexplained suppression is itself a Reviewer finding.

## Tests

One test per `spec.yaml` `contracts:` entry, named so `deepsense/gates/contracts/coverage.py`
can find it by prefix — it collects test names via pytest rather than trusting a
self-reported coverage claim, so the name has to match exactly:

```
contracts.row_count       -> test_contract_row_count*
contracts.uniqueness      -> test_contract_uniqueness*
contracts.not_null        -> test_contract_not_null*
contracts.accepted_values -> test_contract_accepted_values*
contracts.ranges          -> test_contract_ranges*
contracts.freshness       -> test_contract_freshness*
contracts.referential     -> test_contract_referential*
contracts.rejects         -> test_contract_rejects*
```

A contract with no matching test fails the build. Idempotency and schema-conformance are
verified deterministically by the gates (a double-run state hash, an information_schema
comparison) — don't write tests for those; they belong to the gate stack, not the suite.

## Fixtures

Synthesized from the schema, never sampled from a live source — a synthetic fixture can
construct the edge case that matters (a sentinel value, an out-of-range amount, a
duplicate key) instead of hoping a real sample happens to contain it, independent of
whatever data-sensitivity policy eventually applies here.
