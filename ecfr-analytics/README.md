# eCFR Analytics — BigQuery + FastAPI + Tiny UI

A compact, runnable bundle for a take‑home: download eCFR content, flatten to sections,
load into BigQuery, expose a few analytics APIs, and show a minimal browser UI.

**What you get**
- Ingestion (`ingestion/ecfr_ingest.py`) — pulls Parts for selected Titles and flattens to **section‑level** NDJSON with metrics & hashes.
- BigQuery schema + rollups (`infra/bigquery_schema.sql`, `infra/views.sql`).
- FastAPI service (`api/main.py`) — endpoints for wordcount, checksums, diffs, and part details.
- Static UI (`ui/index.html`, `ui/app.js`) — hits the FastAPI endpoints.
- Quick load helper (`scripts/load_ndjson.sh`).

> This uses the public **eCFR Versioner** endpoints (no API key).
> Pick a point‑in‑time `--date` (YYYY‑MM‑DD) for reproducible snapshots.

---

## 0) Prereqs

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) Python package manager
- Google Cloud SDK **or** a service account with BigQuery access
- Set `PROJECT_ID` environment variable or create `.env` file (see `.env.example`)

---

## 1) Ingest eCFR Data

The ingestor now handles BigQuery setup automatically! Choose your preferred mode:

### Option A: Direct to BigQuery (Recommended)
```bash
cd ingestion
uv sync

# Copy and edit environment file
cp .env.example .env
# Edit .env to set your PROJECT_ID

# Ingest directly to BigQuery (creates dataset/tables automatically)
uv run python ecfr_ingest.py --date 2025-08-01 --titles 21 --bigquery --create-rollups

# Multiple titles with custom dataset/table names
uv run python ecfr_ingest.py --date 2025-08-01 --titles 21 40 49 --bigquery --dataset my_ecfr --table sections_test --create-rollups
```

### Option B: NDJSON Files (Traditional)
```bash
cd ingestion
uv sync

# Generate NDJSON files
uv run python ecfr_ingest.py --date 2025-08-01 --titles 21

# Output: ./data/ecfr_sections_2025-08-01.ndjson
# Then load with: ./scripts/load_ndjson.sh YOUR_PROJECT_ID ecfr sections ./ingestion/data/ecfr_sections_2025-08-01.ndjson
```

### Benefits of BigQuery Mode:
- Automatically creates datasets and tables if they don't exist
- No manual `bq` CLI commands needed
- Batched loading for better performance
- Optional rollup table creation
- Schema matches exactly with `infra/bigquery_schema.sql`

> The script walks **structure → parts** then fetches **full part content** and emits section‑rows.

---

## 2) Run the API (FastAPI + Uvicorn)

```bash
cd api
uv sync

# set env (copy .env.example → .env and edit)
export PROJECT_ID=YOUR_PROJECT_ID
export DATASET=ecfr
export TABLE=sections

# if you use a service account locally:
# export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json

uv run uvicorn main:app --reload --port 8000
```

APIs (examples):
- `/api/agency/wordcount?date=2025-08-01`
- `/api/agency/checksum?date=2025-08-01`
- `/api/changes?from=2025-07-01&to=2025-08-01`
- `/api/part?title=21&part=101&date=2025-08-01`

---

## 3) Run the tiny UI

Serve `ui/` any way you like, or just open `ui/index.html` and set the API base at the top of `ui/app.js`.

```bash
# simple Python static server
cd ui
python -m http.server 8080
# open http://localhost:8080
```

---

## Notes & trade‑offs

- **Grain:** section-level rows enable clean diffs + rollups.
- **Partitioning:** table is partitioned by `version_date` for historical analyses.
- **Agency mapping:** we pull a best‑effort `agency_name` from Chapter labels — good enough for dashboards.
- **Robust parsing:** eCFR JSON varies slightly by node type; the ingestor walks nodes defensively and
  extracts sections/paragraphs without relying on a brittle schema.
- **Custom metrics included:** obligation and cross‑reference densities per 1k words.
- **Checksums:** stable SHA‑256 over normalized text; part/agency hashes are ordered aggs of child hashes.

---

## License
Public domain (U.S. Government works + glue code here released CC0).

