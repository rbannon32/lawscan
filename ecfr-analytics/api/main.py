from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List
import os
from dotenv import load_dotenv
from datetime import datetime, date, timedelta

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
    SELECT section_citation, section_heading, section_order, word_count,
           regulatory_burden_score, prohibition_count, requirement_count, enforcement_terms
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

# ========================== ENHANCED HISTORICAL ENDPOINTS ==========================

@app.get("/api/historical/agency-trends")
def agency_trends(
    start_date: str = Query(..., description="Start date YYYY-MM-DD"),
    end_date: str = Query(..., description="End date YYYY-MM-DD"),
    agency: Optional[str] = Query(None, description="Filter by agency name")
):
    """Get historical word count trends by agency over time."""
    where_clause = "WHERE version_date BETWEEN DATE(@start) AND DATE(@end)"
    if agency:
        where_clause += " AND agency_name = @agency"
    
    sql = f"""
    SELECT 
        version_date,
        agency_name,
        SUM(word_count) AS total_words,
        AVG(regulatory_burden_score) AS avg_burden_score,
        SUM(prohibition_count) AS total_prohibitions,
        SUM(requirement_count) AS total_requirements,
        SUM(enforcement_terms) AS total_enforcement_terms,
        COUNT(DISTINCT part_num) AS parts_count,
        COUNT(*) AS sections_count
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    {where_clause}
    GROUP BY version_date, agency_name
    ORDER BY version_date, agency_name
    """
    
    params = [
        bigquery.ScalarQueryParameter("start", "STRING", start_date),
        bigquery.ScalarQueryParameter("end", "STRING", end_date),
    ]
    if agency:
        params.append(bigquery.ScalarQueryParameter("agency", "STRING", agency))
    
    job = bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
    return [dict(r) for r in job.result()]

@app.get("/api/historical/regulatory-burden")
def regulatory_burden_trends(
    start_date: str = Query(..., description="Start date YYYY-MM-DD"),
    end_date: str = Query(..., description="End date YYYY-MM-DD"),
    top_n: int = Query(10, description="Top N agencies by burden score")
):
    """Get regulatory burden trends for top agencies."""
    sql = f"""
    WITH agency_avg_burden AS (
        SELECT 
            agency_name,
            AVG(regulatory_burden_score) AS avg_burden
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE version_date BETWEEN DATE(@start) AND DATE(@end)
        GROUP BY agency_name
        ORDER BY avg_burden DESC
        LIMIT @top_n
    ),
    trends AS (
        SELECT 
            t.version_date,
            t.agency_name,
            AVG(t.regulatory_burden_score) AS avg_burden_score,
            SUM(t.prohibition_count) AS total_prohibitions,
            SUM(t.requirement_count) AS total_requirements,
            SUM(t.enforcement_terms) AS total_enforcement,
            COUNT(*) AS sections_count
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}` t
        INNER JOIN agency_avg_burden a ON t.agency_name = a.agency_name
        WHERE t.version_date BETWEEN DATE(@start) AND DATE(@end)
        GROUP BY t.version_date, t.agency_name
    )
    SELECT * FROM trends
    ORDER BY version_date, avg_burden_score DESC
    """
    
    job = bq.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start", "STRING", start_date),
            bigquery.ScalarQueryParameter("end", "STRING", end_date),
            bigquery.ScalarQueryParameter("top_n", "INT64", top_n),
        ]
    ))
    return [dict(r) for r in job.result()]

@app.get("/api/historical/change-velocity")
def change_velocity(
    start_date: str = Query(..., description="Start date YYYY-MM-DD"),
    end_date: str = Query(..., description="End date YYYY-MM-DD"),
    window_months: int = Query(3, description="Rolling window in months")
):
    """Get regulatory change velocity (sections changing per month)."""
    sql = f"""
    WITH section_changes AS (
        SELECT 
            section_citation,
            version_date,
            section_hash,
            LAG(section_hash) OVER (
                PARTITION BY section_citation 
                ORDER BY version_date
            ) AS prev_hash
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE version_date BETWEEN DATE(@start) AND DATE(@end)
    ),
    monthly_changes AS (
        SELECT 
            DATE_TRUNC(version_date, MONTH) AS month,
            COUNT(DISTINCT section_citation) AS sections_changed
        FROM section_changes
        WHERE section_hash != COALESCE(prev_hash, '')
        GROUP BY month
    )
    SELECT 
        month,
        sections_changed,
        AVG(sections_changed) OVER (
            ORDER BY month 
            ROWS BETWEEN @window_months-1 PRECEDING AND CURRENT ROW
        ) AS rolling_avg_changes
    FROM monthly_changes
    ORDER BY month
    """
    
    job = bq.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start", "STRING", start_date),
            bigquery.ScalarQueryParameter("end", "STRING", end_date),
            bigquery.ScalarQueryParameter("window_months", "INT64", window_months),
        ]
    ))
    return [dict(r) for r in job.result()]

@app.get("/api/metrics/burden-distribution")
def burden_distribution(date: str = Query(..., description="Date YYYY-MM-DD")):
    """Get distribution of regulatory burden scores for a specific date."""
    sql = f"""
    SELECT 
        agency_name,
        COUNT(*) as sections_count,
        AVG(regulatory_burden_score) as avg_burden,
        APPROX_QUANTILES(regulatory_burden_score, 2)[SAFE_OFFSET(1)] as median_burden,
        MAX(regulatory_burden_score) as max_burden,
        SUM(prohibition_count) as total_prohibitions,
        SUM(requirement_count) as total_requirements,
        SUM(enforcement_terms) as total_enforcement,
        SUM(temporal_references) as total_deadlines,
        SUM(dollar_mentions) as total_cost_refs
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE version_date = DATE(@d)
    GROUP BY agency_name
    ORDER BY avg_burden DESC
    """
    
    job = bq.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("d", "STRING", date)]
    ))
    return [dict(r) for r in job.result()]

@app.get("/api/metrics/cost-analysis")
def cost_analysis(date: str = Query(..., description="Date YYYY-MM-DD")):
    """Analyze sections with cost/financial references."""
    sql = f"""
    SELECT 
        agency_name,
        section_citation,
        section_heading,
        dollar_mentions,
        enforcement_terms,
        regulatory_burden_score,
        word_count
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE version_date = DATE(@d) AND dollar_mentions > 0
    ORDER BY dollar_mentions DESC, enforcement_terms DESC
    LIMIT 50
    """
    
    job = bq.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("d", "STRING", date)]
    ))
    return [dict(r) for r in job.result()]

@app.get("/api/available-dates")
def available_dates():
    """Get all available dates in the dataset."""
    sql = f"""
    SELECT DISTINCT version_date
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    ORDER BY version_date DESC
    """
    job = bq.query(sql)
    return [{"date": str(r["version_date"])} for r in job.result()]

@app.get("/api/agencies")
def agencies(date: Optional[str] = Query(None, description="Date YYYY-MM-DD, defaults to latest")):
    """Get all agencies, optionally for a specific date."""
    if date:
        where_clause = "WHERE version_date = DATE(@d)"
        params = [bigquery.ScalarQueryParameter("d", "STRING", date)]
    else:
        where_clause = ""
        params = []
    
    sql = f"""
    SELECT DISTINCT agency_name
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    {where_clause}
    ORDER BY agency_name
    """
    
    job = bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
    return [dict(r) for r in job.result()]
