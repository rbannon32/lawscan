from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import os
from dotenv import load_dotenv

# BigQuery client
from google.cloud import bigquery

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET = os.getenv("DATASET", "ecfr")
TABLE   = os.getenv("TABLE", "sections")

if not PROJECT_ID:
    raise RuntimeError("Set PROJECT_ID env (or use .env) before starting the API")

bq = bigquery.Client(project=PROJECT_ID)

app = FastAPI(title="eCFR Analytics API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.get("/api/agency/wordcount")
def agency_wordcount(date: str = Query(..., description="YYYY-MM-DD")):
    sql = f"""
    SELECT agency_name, SUM(word_count) AS total_words
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE version_date = DATE(@d)
    GROUP BY agency_name
    ORDER BY total_words DESC
    """
    job = bq.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("d", "STRING", date)]
    ))
    return [dict(r) for r in job.result()]

@app.get("/api/agency/checksum")
def agency_checksum(date: str = Query(..., description="YYYY-MM-DD")):
    sql = f"""
    WITH parts AS (
      SELECT
        version_date,
        agency_name,
        part_num,
        TO_HEX(SHA256(STRING_AGG(section_hash, '' ORDER BY section_citation))) AS part_hash
      FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
      WHERE version_date = DATE(@d)
      GROUP BY version_date, agency_name, part_num
    )
    SELECT
      agency_name,
      TO_HEX(SHA256(STRING_AGG(part_hash, '' ORDER BY part_num))) AS agency_hash
    FROM parts
    GROUP BY agency_name
    ORDER BY agency_name
    """
    job = bq.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("d", "STRING", date)]
    ))
    return [dict(r) for r in job.result()]

@app.get("/api/changes")
def changes(date_from: str = Query(..., alias="from"),
            date_to:   str = Query(..., alias="to")):
    sql = f"""
    WITH last AS (
      SELECT section_citation, section_hash
      FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
      WHERE version_date = DATE(@d1)
    ),
    now AS (
      SELECT section_citation, section_hash
      FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
      WHERE version_date = DATE(@d2)
    )
    SELECT
      COALESCE(now.section_citation, last.section_citation) AS section_citation,
      CASE
        WHEN last.section_hash IS NULL THEN 'ADDED'
        WHEN now.section_hash  IS NULL THEN 'REMOVED'
        WHEN last.section_hash != now.section_hash THEN 'MODIFIED'
        ELSE 'UNCHANGED'
      END AS change_type
    FROM last
    FULL OUTER JOIN now USING (section_citation)
    WHERE NOT (last.section_hash = now.section_hash)
    ORDER BY change_type, section_citation
    """
    job = bq.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("d1", "STRING", date_from),
            bigquery.ScalarQueryParameter("d2", "STRING", date_to),
        ]
    ))
    return [dict(r) for r in job.result()]

@app.get("/api/part")
def part(title: int, part: str, date: str):
    sql = f"""
    SELECT section_citation, section_heading, section_order, word_count
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE version_date = DATE(@d) AND title_num = @t AND part_num = @p
    ORDER BY section_order
    """
    job = bq.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("d", "STRING", date),
            bigquery.ScalarQueryParameter("t", "INT64", title),
            bigquery.ScalarQueryParameter("p", "STRING", part),
        ]
    ))
    return [dict(r) for r in job.result()]
