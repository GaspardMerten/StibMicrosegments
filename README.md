# StibMicrosegments

Platform around the [`microsegments`](../microsegments) package for STIB (Brussels): a nightly
ingest of the MobilityTwin.Brussels STIB feeds into compact Parquet on GCS, then (to come)
derivation and a FastAPI on Cloud Run (`mobility-twin-norse`, europe-west1).

## Ingest locally

```bash
uv venv --python 3.12 && uv pip install -e '.[dev]'     # add ',analysis' for the microsegments package
export MOBILITYTWIN_TOKEN=...                           # or put it in a .env file
.venv/bin/python -m stibms.ingest --date 2025-03-18 --local data/ms
.venv/bin/python -m stibms.ingest --date 2025-03-01 --to 2025-03-31 --bucket gs://mobility-twin-norse-microsegments
```

A date that already has `raw/_SUCCESS/date=D.json` is skipped (`--force` rewrites it).
`--date yesterday` is the nightly run. `--shard-from-env` splits the dates across Cloud Run job tasks.

Layout per service date D (04:00 → 03:00 Europe/Brussels), all lines:

| Path | Content |
|---|---|
| `raw/vd/date=D/vd.parquet` | `ts` (epoch s), `line`, `dir`, `point`, `dist`; sorted by line, ts; row groups by line |
| `raw/vd/date=D/snaps.parquet` | every poll: `ts`, `n_rows`, `frozen` |
| `raw/punctuality/date=D/p.parquet` | stib/punctuality, the columns we use |
| `gtfs/feeds/<sha>/*.parquet`, `gtfs/index.parquet` | GTFS in force on D, stored once per content sha |
| `raw/_SUCCESS/date=D.json` | counts, sources and bytes of the run |

A poll whose content repeats the previous poll is `frozen`. It is listed in `snaps.parquet`, but
its vehicle rows are not repeated in `vd.parquet` (see `src/stibms/vd.py`).

Tests: `pytest` runs offline. `pytest -m network` checks line 55 against the prototype and needs a token.

## Deploy

Infrastructure lives in `terraform/` (bucket, secret, service accounts, WIF pool, the
`ms-ingest` and `ms-backfill` jobs, the scheduler at 05:30 Brussels and the `ms-api` service with
min 0 instances).

```bash
cd terraform && terraform init && terraform apply
printf %s "$MOBILITYTWIN_TOKEN" | gcloud secrets versions add ms-mobilitytwin-token --data-file=- --project mobility-twin-norse
```

After that, every push to `main` builds the image and updates the jobs and the API
(`.github/workflows/deploy.yml`).

To run the backfill:

```bash
gcloud run jobs execute ms-backfill --region europe-west1 --project mobility-twin-norse
```
