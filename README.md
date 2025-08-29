# My Project
# eCFR Analytics — Regulatory Intelligence Platform

A comprehensive regulatory analytics platform that ingests eCFR data, provides historical trend analysis, and delivers professional insights through BigQuery, FastAPI, and a modern web interface with AI-powered conversational analysis.

**What you get**
- **Smart Ingestor** (`ingestion/ecfr_ingest.py`) — XML-based eCFR parser with BigQuery integration and historical backfill capabilities
- **Enhanced Analytics** — Custom regulatory burden scoring, prohibition/requirement counting, enforcement term detection
- **Historical Analysis** — Monthly snapshots from 2017-present with smart amendment-based skipping  
- **Professional API** (`api/main.py`) — 15+ endpoints for comprehensive regulatory analysis including trends, burden distribution, and change velocity
- **AI Assistant** (`ai_service/`) — Vertex AI-powered conversational interface with RAG (Retrieval-Augmented Generation) for regulatory Q&A
- **Modern UI** (`ui/`) — ShadCN-inspired interface with dark/light themes, tabbed navigation, responsive design, and integrated AI chat
- **Automated Infrastructure** — BigQuery dataset/table creation, schema management, and batched loading

> Uses the public **eCFR Versioner** API with robust XML parsing.
> Supports reproducible point-in-time snapshots and comprehensive historical analysis.

---

## Prerequisites

- **Python 3.10+**
- **[uv](https://docs.astral.sh/uv/)** — Modern Python package manager
- **Google Cloud Platform** — Project with BigQuery API enabled
- **Authentication** — Google Cloud SDK configured OR service account JSON

---

## Quick Start

### 1. Setup Environment

```bash
# Clone or extract the project
cd ecfr-analytics

# Setup ingestion environment
cd ingestion
uv sync

# Setup API environment  
cd ../api
uv sync

# Setup AI service environment
cd ../ai_service
uv pip install -r requirements.txt

# Configure BigQuery access
cp .env.example .env
# Edit .env to set your PROJECT_ID and preferred dataset/table names
```

### 2. Authenticate with Google Cloud

```bash
# Option A: Use gcloud CLI (recommended for local development)
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID

# Option B: Use service account (for production/CI)
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# Enable required APIs for AI service
gcloud services enable aiplatform.googleapis.com
gcloud services enable bigquery.googleapis.com

# Create service account for AI service (optional)
gcloud iam service-accounts create ecfr-ai-service
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:ecfr-ai-service@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/aiplatform.user"
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:ecfr-ai-service@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.user"
```

### 3. Ingest Sample Data

```bash
cd ingestion

# Quick test - ingest 3 titles for current date
uv run python ecfr_ingest.py --date 2025-08-22 --titles 3 7 21 --bigquery

# Historical backfill (recommended) - creates rich dataset for analysis
uv run python ecfr_ingest.py --backfill --titles 3 --start-year 2020
```

### 4. Configure API Environment

```bash
cd api
# Edit .env file to add your Gemini API key for AI analysis
echo "PROJECT_ID=your-gcp-project
DATASET=ecfr_enhanced
TABLE=sections_enhanced
GEMINI_API_KEY=your_gemini_api_key_here" > .env
```

### 5. Start the API Server

```bash
cd api
uv run uvicorn main:app --reload --port 8000

# API will be available at http://localhost:8000
# Interactive docs at http://localhost:8000/docs
```

### 6. Start the AI Service (Optional - for conversational AI)

```bash
cd ai_service
# Configure environment (create .env file)
echo "PROJECT_ID=YOUR_PROJECT_ID
REGION=us-central1
DATASET=ecfr_enhanced
TABLE=sections_enhanced
GOOGLE_APPLICATION_CREDENTIALS=~/ecfr-ai-key.json" > .env

# Start the AI service
uv run python main.py
# AI service will be available at http://localhost:8001
```

### 7. Launch the Web Interface

```bash
cd ui
python -m http.server 8080
# Open http://localhost:8080
# AI Assistant tab will connect to the AI service automatically
```

---

## Detailed Usage

### Data Ingestion Options

#### Single Date Ingestion
```bash
cd ingestion

# Basic ingestion for specific date
uv run python ecfr_ingest.py --date 2025-08-22 --titles 3 7 21 --bigquery

# With custom dataset/table names  
uv run python ecfr_ingest.py --date 2025-08-22 --titles 21 --bigquery \
  --dataset my_ecfr --table enhanced_sections --create-rollups
```

#### Historical Backfill (Recommended)
```bash
# Full historical analysis (2017-present, monthly snapshots)
uv run python ecfr_ingest.py --backfill --titles 3 7 21 --start-year 2017

# Recent history only
uv run python ecfr_ingest.py --backfill --titles 40 --start-year 2023

# Custom date range
uv run python ecfr_ingest.py --backfill --titles 21 \
  --start-date 2023-01-01 --end-date 2024-12-31
```

#### Command Line Options
- `--titles` — CFR titles to ingest (space-separated integers)
- `--date` — Specific date (YYYY-MM-DD) for single ingestion
- `--backfill` — Enable historical backfill mode
- `--start-year` / `--start-date` — Backfill start point  
- `--end-date` — Backfill end point (defaults to current date)
- `--bigquery` — Load directly to BigQuery (auto-creates tables)
- `--dataset` — BigQuery dataset name (default: ecfr_enhanced)
- `--table` — BigQuery table name (default: sections_enhanced)
- `--create-rollups` — Generate summary tables

### API Configuration

```bash
cd api

# Environment variables (or use .env file)
export PROJECT_ID=your-gcp-project
export DATASET=ecfr_enhanced  
export TABLE=sections_enhanced

# Optional service account
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json

# Start with custom host/port
uv run uvicorn main:app --host 0.0.0.0 --port 8001
```

#### Key API Endpoints

**Core Analytics**
- `GET /api/agency/wordcount?date=2025-08-28` — Word counts by agency
- `GET /api/agency/checksum?date=2025-08-28` — Content checksums  
- `GET /api/part?title=3&part=100&date=2025-08-28` — Detailed part contents

**Enhanced Browsing**
- `GET /api/browse/titles?date=2025-08-28` — Browse all CFR titles with statistics
- `GET /api/browse/parts?title=7&date=2025-08-28` — Browse parts within a title
- `GET /api/browse/sections?title=7&part=100&date=2025-08-28&sort_by=burden` — Browse sections with sorting
- `GET /api/browse/search?query=office&date=2025-08-28` — Search across regulations

**AI-Powered Analysis**
- `POST /api/ai/analyze-section` — Get AI analysis of specific regulation sections with historical context, complexity assessment, and improvement recommendations

**Advanced Metrics**
- `GET /api/metrics/burden-distribution?date=2025-08-28` — Regulatory burden analysis
- `GET /api/metrics/cost-analysis?date=2025-08-28` — Sections with financial references
- `GET /api/section/text?title=7&part=100&section=7 CFR § 100.1&date=2025-08-28` — Full section text and metrics
- `GET /api/agencies?date=2025-08-28` — Agency list

### AI Assistant Features

The integrated AI Assistant provides:

- **Conversational Interface** — Natural language queries about federal regulations
- **RAG Architecture** — Retrieval-Augmented Generation with full regulatory context
- **Regulatory Expertise** — AI trained on CFR hierarchy, burden scoring, and compliance terminology
- **Source Citations** — Every response includes specific CFR section references
- **Semantic Search** — Intelligent keyword and concept matching across regulations
- **Conversation History** — Context-aware follow-up questions and clarifications
- **Real-time Status** — Connection monitoring with visual AI service indicators

**Example Queries:**
- "What are the ethical conduct requirements for federal employees?"
- "Show me environmental regulations with high regulatory burden scores"
- "Explain the enforcement mechanisms in Title 21"

### Web Interface Features

The modern web interface provides:

- **Overview Tab** — Agency word counts, checksums, and regulatory burden analysis with interactive tooltips
- **Part Browser Tab** — Enhanced browsing with search, sorting, and detailed section analysis
- **AI Assistant Tab** — Conversational interface with regulatory expertise and source citations  
- **Section-level AI Analysis** — Click "Ask AI" on any section for detailed analysis including:
  - Historical context and regulatory purpose
  - Complexity assessment for compliance officers
  - Regulatory burden evaluation with score justification
  - Necessity analysis for public interest protection
  - Specific improvement recommendations
- **Interactive Regulatory Burden Tooltips** — Hover over any burden score to see detailed calculation methodology
- **Enhanced Search** — Full-text search across all regulations with burden score filtering
- **Dark/Light Theme** — Automatic system preference detection with manual toggle
- **Responsive Design** — Professional ShadCN-inspired components
- **Real-time API Status** — Connection monitoring with visual indicators for both API and AI services
- **Enhanced Tables** — Smart formatting, loading states, and error handling

### Advanced Configuration

#### Custom BigQuery Schema
The enhanced schema includes custom regulatory metrics and AI-optimized fields:

```sql
-- Regulatory burden scoring (0-100 scale)
regulatory_burden_score FLOAT64,

-- Content analysis metrics  
prohibition_count INT64,
requirement_count INT64,
enforcement_terms INT64,
temporal_references INT64,
dollar_mentions INT64,

-- AI-optimized fields for RAG
ai_context_summary STRING,      -- AI-generated context summaries
embedding_optimized_text STRING -- Text optimized for vector embeddings
```

#### Performance Optimization

**Monthly Backfill Strategy**
- Uses amendment dates to skip unchanged months
- Processes ~100 months instead of 3000+ days
- Batched BigQuery loading for optimal performance

**Smart Caching**
- Content hashes prevent duplicate processing
- Amendment date tracking for efficient updates
- Partitioned tables for fast historical queries

---

## Troubleshooting

### Common Issues

**BigQuery Authentication Errors**
```bash
# Check authentication status
gcloud auth list
gcloud config get-value project

# Re-authenticate if needed
gcloud auth application-default login
```

**API Connection Issues**
```bash
# Verify API is running
curl http://localhost:8000/healthz

# Check BigQuery connectivity
curl "http://localhost:8000/api/available-dates"

# Verify AI service is running
curl http://localhost:8001/health

# Test AI service chat endpoint
curl -X POST http://localhost:8001/chat \
  -H "Content-Type: application/json" \
  -d '{"message":"office"}'
```

**Empty Data Results**
- Ensure you've run ingestion for the dates you're querying
- Check that your `.env` file points to the correct dataset/table  
- Verify BigQuery permissions on your project

**UI Not Loading**
- Check browser console for JavaScript errors
- Verify API_BASE setting in `ui/app.js` matches your API server
- Ensure CORS is working (should be enabled by default)

**AI Analysis Issues**
- **Gemini API**: Ensure you have a valid `GEMINI_API_KEY` in your API `.env` file
- **API Quota**: Check Google AI Studio for API usage limits
- **Missing Analysis**: Verify the section exists and contains text data
- **Connection Error**: Ensure the API server is running with the updated environment

**Conversational AI Assistant Issues** (Optional service)
- Verify Vertex AI APIs are enabled: `gcloud services list --enabled | grep aiplatform`
- Check service account has required roles: `roles/aiplatform.user` and `roles/bigquery.user`
- Ensure AI context columns exist: Run `ALTER TABLE` commands to add `ai_context_summary` and `embedding_optimized_text`
- Check available data matches your queries: Current dataset may only contain specific CFR titles

### Development Tips

**Testing Changes**
```bash
# Quick single-title test
uv run python ecfr_ingest.py --date 2025-08-28 --titles 3 --bigquery

# Test AI analysis endpoint
curl -X POST "http://localhost:8000/api/ai/analyze-section" \
  -H "Content-Type: application/json" \
  -d '{"section_citation":"48 CFR § 32.204","title":"48","part":"32","date":"2025-08-28"}'
```

**Performance Monitoring**
- BigQuery job history: https://console.cloud.google.com/bigquery/jobs
- API logs: Check uvicorn output for query timing  
- Browser network tab: Monitor API response times

---

## Technical Architecture

### Data Pipeline
1. **eCFR Versioner API** → XML parsing → Section extraction
2. **Content Analysis** → Custom metrics calculation → BigQuery batching  
3. **Historical Backfill** → Amendment-based change detection → Monthly snapshots
4. **API Layer** → Query optimization → JSON responses
5. **Web Interface** → Interactive analytics → Professional UI

### Key Design Decisions

- **Section-level granularity** enables precise change tracking and flexible rollups
- **Monthly snapshots** balance completeness with processing efficiency  
- **Custom regulatory metrics** provide decision-making insights beyond raw text
- **BigQuery partitioning** by `version_date` optimizes historical analysis queries
- **XML parsing with fallbacks** handles API evolution and edge cases
- **Batched loading** minimizes BigQuery API calls and costs
- **Professional UI components** create production-ready presentation layer

### Schema Design

The enhanced schema supports sophisticated regulatory analysis:

```sql
CREATE TABLE sections_enhanced (
  version_date DATE,
  title_num INT64,
  part_num STRING, 
  section_citation STRING,
  section_heading STRING,
  agency_name STRING,
  word_count INT64,
  section_hash STRING,
  
  -- Custom regulatory metrics
  regulatory_burden_score FLOAT64,  -- 0-100 composite score
  prohibition_count INT64,          -- "shall not", "prohibited" 
  requirement_count INT64,          -- "shall", "must", "required"
  enforcement_terms INT64,          -- "penalty", "violation", "fine"
  temporal_references INT64,        -- Deadlines and time constraints  
  dollar_mentions INT64,            -- Cost and financial references
  
  -- Content analysis
  obligation_density FLOAT64,       -- Obligations per 1k words
  cross_reference_density FLOAT64   -- Citations per 1k words
)
PARTITION BY version_date;
```

---

## License

Public domain (U.S. Government works + glue code released under CC0).

