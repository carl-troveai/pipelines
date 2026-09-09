# sources

Committed local files for `local_file`-type ETL requests (`.github/ISSUE_TEMPLATE/etl_request.yml`).

Commit the file here yourself before filing the issue, then reference its path
relative to the repo root (`sources/customers_export.csv`, not `customers_export.csv`)
in the issue's **Source location** field.

`pipelines/<name>/` doesn't exist until the Architect names it, so a local file has
nowhere else stable to live before the pipeline itself exists. The Profiler queries it
directly at this path (e.g. `read_csv('sources/customers_export.csv')`,
`read_parquet(...)`) - no separate upload mechanism, no credential, just a file checked
into version control like anything else in this repo.
