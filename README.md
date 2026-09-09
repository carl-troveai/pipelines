# pipelines

**This is Repo B: generated code.** deepsense's agents write here; they cannot write to
[`deepsense/`](https://github.com/carl-troveai/deepsense) (Repo A, the harness — prompts, gates, the merge bot). The
agent's GitHub token is scoped to this repo only. See `deepsense/docs/architecture.md` §2
for why the split exists.

## Layout

```
pipelines/<name>/
    ADR.md              why this stack was chosen
    spec.yaml           the contract (source -> target)
    profile.json        what the data actually looks like (source pipelines only)
    deepsense_entry.py  required uniform entrypoint - see AGENTS.md
    <implementation>    polars module, dbt project, pyspark job, whatever the ADR chose
    tests/              one test per spec.yaml contracts: entry
    fixtures/           synthetic clean + malformed data, never sampled from live rows
sources/                committed files for local_file-type ETL requests - see AGENTS.md
.github/ISSUE_TEMPLATE/etl_request.yml
AGENTS.md               house conventions every pipeline in this repo follows
```

`AGENTS.md` lives once at this repo's root, not per pipeline - conventions are shared
across every `pipelines/<name>/`.

## Verifying a pipeline locally

From the `deepsense/` checkout:

```bash
uv run python -m gates.runner --pipeline <name> --repo-root ../pipelines
```

See `deepsense/gates/README.md` for what each gate checks and the break-test table used
to validate the gate stack itself before trusting it.
