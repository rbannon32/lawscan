#!/usr/bin/env python3
"""
eCFR → BigQuery Ingestor (per-Part, per-Date)

Features
- Discovers parts for a Title and date (latest if not provided)
- Downloads full XML for each Part and extracts simple plain text
- Idempotent upsert: skips rows that already exist in BigQuery
- Metadata: checksum, sizes, source URL, timestamps
- Optional GCS offload if XML too large for a BigQuery STRING cell

Environment
- GCP_PROJECT      : your GCP project id (required)
- BQ_DATASET       : dataset name (default: ecfr)
- BQ_TABLE         : table name (default: parts)
- GCS_BUCKET       : optional bucket (for large XML offload)
- GOOGLE_APPLICATION_CREDENTIALS or ADC from `gcloud auth application-default login`

CLI Examples
  python ecfr_ingest_bq.py --title 21 --parts 1308 --date 2025-08-22
  python ecfr_ingest_bq.py --title 21                          # latest date, all parts
  python ecfr_ingest_bq.py --title 40 --parts 50 600 --out ./cache

Run with uv:
  uv run --with requests --with beautifulsoup4 --with lxml --with google-cloud-bigquery --with google-cloud-storage \
    python ecfr_ingest_bq.py --title 21 --parts 1308 --date 2025-08-22
"""

import argparse
import hashlib
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Google Cloud ---
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
try:
    from google.cloud import storage  # optional (GCS offload)
except Exception:
    storage = None  # we'll guard it

# ----------------- eCFR fetch layer -----------------
BASE = "https://www.ecfr.gov"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "ecfr-bq-ingestor/1.0 (contact: you@example.com)"
})

retry = Retry(
    total=5,
    connect=5,
    read=5,
    backoff_factor=1.0,  # 0,1,2,4,8s
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "HEAD", "OPTIONS"],
    raise_on_status=False,
)
adapter = HTTPAdapter(max_retries=retry)
SESSION.mount("https://", adapter)
SESSION.mount("http://", adapter)

THROTTLE_SECONDS = 0.5  # polite throttle


def _sleep():
    time.sleep(THROTTLE_SECONDS)


def get_json(url: str) -> dict:
    _sleep()
    r = SESSION.get(url, timeout=60)
    r.raise_for_status()
    return r.json()


def get_text(url: str) -> str:
    _sleep()
    r = SESSION.get(url, timeout=180)
    r.raise_for_status()
    return r.text


def list_titles() -> List[Dict]:
    data = get_json(f"{BASE}/api/versioner/v1/titles.json")
    titles = data.get("titles", [])
    meta = data.get("meta", {})
    print(f"Titles meta: date={meta.get('date')} import_in_progress={meta.get('import_in_progress')}")
    for t in titles:
        print(f"Title {t.get('number'):>2}: {t.get('name')} "
              f"| latest_amended_on={t.get('latest_amended_on')} "
              f"latest_issue_date={t.get('latest_issue_date')} up_to_date_as_of={t.get('up_to_date_as_of')}")
    return titles


def latest_version_date_for_title(title: int) -> str:
    # Try versions endpoint first
    try:
        data = get_json(f"{BASE}/api/versioner/v1/versions/title-{title}.json")
        versions = (data.get("versions")
                    or data.get("dates")
                    or data.get("version_dates")
                    or [])
        if versions:
            return max(versions)  # ISO dates
    except Exception:
        pass

    # Fallback to titles listing (up_to_date_as_of)
    titles = get_json(f"{BASE}/api/versioner/v1/titles.json").get("titles", [])
    for t in titles:
        try:
            if int(t.get("number")) == int(title):
                return (t.get("up_to_date_as_of") or t.get("latest_issue_date") or t.get("latest_amended_on"))
        except Exception:
            continue
    raise RuntimeError(f"Could not determine a version date for title {title}")


def get_structure(title: int, date: str) -> dict:
    return get_json(f"{BASE}/api/versioner/v1/structure/{date}/title-{title}.json")


def iter_parts_from_structure(struct: dict) -> List[str]:
    parts = set()

    def walk(node: dict):
        node_type = (node.get("type") or "").lower()
        label = node.get("label") or ""
        if node_type == "part":
            ident = node.get("identifier") or ""
            m = re.search(r"Part\s+([0-9A-Za-z\-]+)", label, re.IGNORECASE)
            if ident:
                parts.add(str(ident))
            elif m:
                parts.add(m.group(1))
        for child in node.get("children", []):
            walk(child)

    if "nodes" in struct and isinstance(struct["nodes"], list):
        for n in struct["nodes"]:
            walk(n)
    else:
        walk(struct)
    return sorted(parts, key=lambda x: (len(x), x))


def download_part_xml(title: int, date: str, part_id: str) -> Tuple[str, str]:
    url = f"{BASE}/api/versioner/v1/full/{date}/title-{title}.xml?part={part_id}"
    return get_text(url), url


def xml_to_plain_text(xml_text: str) -> str:
    soup = BeautifulSoup(xml_text, "lxml-xml")  # requires lxml
    blocks = []
    for tag in soup.find_all([
        "HD", "HEAD", "HED",
        "DIV1", "DIV2", "DIV3", "DIV4", "DIV5", "DIV6", "DIV7", "DIV8",
        "SECTNO", "SUBJECT", "P", "FP"
    ]):
        tx = tag.get_text(separator=" ", strip=True)
        if tx:
            blocks.append(tx)
    if not blocks:
        blocks = [soup.get_text(separator="\n", strip=True)]
    return "\n".join(blocks)


def is_real_part(p: str) -> bool:
    return p.isdigit()  # skip ranges like "83-98" and alphas for batch runs


def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


# ----------------- BigQuery / GCS layer -----------------

BQ_MAX_CELL = 9_000_000  # ~9MB safety to stay under 10MB STRING limit

def gcs_upload(bucket_name: str, key: str, data: bytes) -> str:
    if not storage:
        raise RuntimeError("google-cloud-storage not available; install and try again.")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(key)
    blob.upload_from_string(data, content_type="application/xml")
    return f"gs://{bucket_name}/{key}"


def ensure_table(client: bigquery.Client, dataset: str, table: str):
    ds_id = f"{client.project}.{dataset}"
    tb_id = f"{client.project}.{dataset}.{table}"

    # Create dataset if missing
    try:
        client.get_dataset(ds_id)
    except NotFound:
        client.create_dataset(bigquery.Dataset(ds_id))
        print(f"Created dataset: {ds_id}")

    # Create table if missing
    try:
        client.get_table(tb_id)
        return
    except NotFound:
        schema = [
            bigquery.SchemaField("title_number", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("version_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("part_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("part_title", "STRING"),
            bigquery.SchemaField("xml", "STRING"),                # may be NULL if offloaded
            bigquery.SchemaField("text", "STRING"),
            bigquery.SchemaField("gcs_xml_uri", "STRING"),
            bigquery.SchemaField("source_url", "STRING"),
            bigquery.SchemaField("checksum_sha256", "STRING"),
            bigquery.SchemaField("xml_size_bytes", "INT64"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        ]
        table_obj = bigquery.Table(tb_id, schema=schema)
        # Partitioning/sharding optional; for now, plain table
        client.create_table(table_obj)
        print(f"Created table: {tb_id}")


def row_exists(client: bigquery.Client, dataset: str, table: str,
               title_number: int, version_date: str, part_id: str) -> bool:
    tb_id = f"{client.project}.{dataset}.{table}"
    q = f"""
    SELECT 1
    FROM `{tb_id}`
    WHERE title_number = @title
      AND version_date = @date
      AND part_id = @part
    LIMIT 1
    """
    job = client.query(
        q,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("title", "INT64", title_number),
                bigquery.ScalarQueryParameter("date", "DATE", version_date),
                bigquery.ScalarQueryParameter("part", "STRING", part_id),
            ]
        ),
    )
    return job.result().total_rows > 0


def upsert_row(client: bigquery.Client, dataset: str, table: str, row: dict):
    """
    Simple idempotent behavior: check existence, insert if missing.
    For large backfills, you can batch rows with insert_rows_json, or stage & MERGE.
    """
    if row_exists(client, dataset, table, row["title_number"], row["version_date"], row["part_id"]):
        print(f"[skip] already in BQ: title={row['title_number']} date={row['version_date']} part={row['part_id']}")
        return

    tb_id = f"{client.project}.{dataset}.{table}"
    errors = client.insert_rows_json(tb_id, [row])
    if errors:
        raise RuntimeError(f"BigQuery insert error: {errors}")


def maybe_extract_part_title(xml_text: str) -> Optional[str]:
    # Try to pull a human-friendly Part heading if present
    try:
        soup = BeautifulSoup(xml_text, "lxml-xml")
        # Common patterns: a heading with the Part label; keep it best-effort
        hd = soup.find(["HD", "HEAD", "HED"])
        if hd:
            t = hd.get_text(separator=" ", strip=True)
            # Limit length to something reasonable
            return t[:1000]
    except Exception:
        pass
    return None


# ----------------- Main ingest flow -----------------

def run_ingest(
    title: int,
    parts: Optional[List[str]],
    date: Optional[str],
    out_dir: str,
    list_titles_only: bool = False,
):
    # GCP config
    project = os.environ.get("GCP_PROJECT")
    if not project:
        print("ERROR: Set GCP_PROJECT in env", file=sys.stderr)
        sys.exit(2)
    dataset = os.environ.get("BQ_DATASET", "ecfr")
    table = os.environ.get("BQ_TABLE", "parts")
    gcs_bucket = os.environ.get("GCS_BUCKET")  # optional

    bq = bigquery.Client(project=project)
    ensure_table(bq, dataset, table)

    if list_titles_only:
        list_titles()
        return

    # Resolve date
    version_date = date or latest_version_date_for_title(title)

    # Discover parts
    struct = get_structure(title, version_date)
    all_parts = iter_parts_from_structure(struct)

    # Determine targets
    if parts and len(parts) > 0:
        target_parts = [str(p) for p in parts]
        # optional filter to numeric ids
        target_parts = [p for p in target_parts if is_real_part(p)]
    else:
        target_parts = [p for p in all_parts if is_real_part(p)]

    # Optional local cache dir (good for resuming & inspection)
    out_path = Path(out_dir or "./ecfr_out") / f"title-{title}" / version_date
    out_path.mkdir(parents=True, exist_ok=True)

    print(f"Title {title} — version date {version_date}")
    print(f"Parts to ingest: {'(none)' if not target_parts else ', '.join(target_parts)}")
    print(f"BigQuery target: {project}.{dataset}.{table}")
    if gcs_bucket:
        print(f"GCS offload bucket: gs://{gcs_bucket}")

    ingested = 0
    for part in target_parts:
        xml_fp = out_path / f"part-{part}.xml"
        txt_fp = out_path / f"part-{part}.txt"

        # Skip if already in BQ (fast resume)
        if row_exists(bq, dataset, table, title, version_date, part):
            print(f"[skip/bq] title={title} date={version_date} part={part}")
            continue

        # Fetch (or reuse cached)
        if xml_fp.exists():
            xml_text = xml_fp.read_text(encoding="utf-8", errors="ignore")
            source_url = f"{BASE}/api/versioner/v1/full/{version_date}/title-{title}.xml?part={part}"
        else:
            print(f"[get ] part {part} XML...")
            try:
                xml_text, source_url = download_part_xml(title, version_date, part)
                xml_fp.write_text(xml_text, encoding="utf-8")
            except requests.HTTPError as e:
                code = getattr(e.response, "status_code", "unknown")
                print(f"[warn] part {part}: HTTP {code} — skipping")
                continue
            except Exception as e:
                print(f"[warn] part {part}: fetch failed ({e}) — skipping")
                continue

        # Plain text extraction (cache)
        if txt_fp.exists():
            text = txt_fp.read_text(encoding="utf-8", errors="ignore")
        else:
            try:
                text = xml_to_plain_text(xml_text)
                txt_fp.write_text(text, encoding="utf-8")
            except Exception as ex:
                print(f"[warn] part {part}: XML→text failed ({ex}); storing XML only.")
                text = None

        # Metadata & offload handling
        checksum = sha256_hex(xml_text)
        xml_bytes = xml_text.encode("utf-8", errors="ignore")
        xml_size = len(xml_bytes)
        gcs_uri = None
        xml_for_bq: Optional[str] = xml_text

        if xml_size > BQ_MAX_CELL:
            if not gcs_bucket:
                print(f"[warn] part {part}: XML {xml_size} bytes too large for BQ cell and no GCS_BUCKET set — will store NULL XML.")
                xml_for_bq = None
            else:
                # Upload to GCS and store URI
                ymd = version_date.replace("-", "")
                key = f"ecfr/title-{title}/{ymd}/part-{part}.xml"
                try:
                    gcs_uri = gcs_upload(gcs_bucket, key, xml_bytes)
                    xml_for_bq = None  # keep BQ cell small
                    print(f"[gcs ] {gcs_uri}")
                except Exception as ex:
                    print(f"[warn] GCS upload failed ({ex}); storing XML inline if possible.")
                    if xml_size > BQ_MAX_CELL:
                        xml_for_bq = None

        part_title = maybe_extract_part_title(xml_text)

        row = {
            "title_number": title,
            "version_date": version_date,
            "part_id": part,
            "part_title": part_title,
            "xml": xml_for_bq,
            "text": text,
            "gcs_xml_uri": gcs_uri,
            "source_url": source_url,
            "checksum_sha256": checksum,
            "xml_size_bytes": xml_size,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }

        try:
            upsert_row(bq, dataset, table, row)
            print(f"[done] inserted: title={title} date={version_date} part={part} size={xml_size}")
            ingested += 1
        except Exception as ex:
            print(f"[error] BQ insert failed for part {part}: {ex}")

    print(f"\nCompleted. Ingested {ingested} part(s) to BigQuery.")


def main():
    ap = argparse.ArgumentParser(description="Ingest eCFR Part XML + Text into BigQuery")
    ap.add_argument("--list-titles", action="store_true", help="List titles/metadata and exit.")
    ap.add_argument("--title", type=int, help="CFR Title number (e.g., 21, 40).")
    ap.add_argument("--parts", nargs="*", help="Specific Part numbers (e.g., 1308 807). If omitted, ingests ALL parts.")
    ap.add_argument("--date", type=str, help="Version date YYYY-MM-DD (default: latest).")
    ap.add_argument("--out", type=str, default="./ecfr_out", help="Local cache dir for XML/TXT.")
    args = ap.parse_args()

    if args.list_titles:
        list_titles()
        return

    if not args.title:
        print("Please provide --title N or --list-titles", file=sys.stderr)
        sys.exit(2)

    run_ingest(
        title=args.title,
        parts=args.parts,
        date=args.date,
        out_dir=args.out,
        list_titles_only=False,
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user.", file=sys.stderr)
        sys.exit(130)
