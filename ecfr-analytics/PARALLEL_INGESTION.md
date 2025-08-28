# Parallel eCFR Ingestion with GCP Cloud Functions

## Overview

This system deploys multiple GCP Cloud Functions to ingest eCFR data in parallel, dramatically reducing ingestion time from hours to minutes.

## Architecture

- **Cloud Function**: `ecfr-ingest-part` - Processes a single CFR part
- **Orchestrator**: Python script that coordinates deployment and execution
- **Concurrency**: Configurable batch processing (default: 20 concurrent functions)
- **Auto-cleanup**: Removes Cloud Functions after completion

## Files Structure

```
cloud_functions/
├── ecfr_ingest_part/
│   ├── main.py              # Cloud Function code
│   ├── requirements.txt     # Dependencies
│   └── .env.yaml           # Environment variables

scripts/
├── deploy_parallel_ingestion.py  # Orchestration script
├── run_parallel_ingestion.sh     # Easy deployment script
└── verify_ecfr.py                # Data verification
```

## Quick Start

### 1. Prerequisites

```bash
# Install Google Cloud SDK
# https://cloud.google.com/sdk/docs/install

# Authenticate
gcloud auth login
gcloud config set project lawscan

# Enable required APIs (auto-enabled by script)
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable cloudbuild.googleapis.com  
gcloud services enable bigquery.googleapis.com
```

### 2. Run Parallel Ingestion

**Simple usage (Title 7 with defaults):**
```bash
cd /Users/ryanbannon/coding/lawscan/ecfr-analytics
./scripts/run_parallel_ingestion.sh
```

**Custom parameters:**
```bash
./scripts/run_parallel_ingestion.sh TITLE DATE BATCH_SIZE
./scripts/run_parallel_ingestion.sh 7 2025-08-22 30
```

**Advanced usage with Python script:**
```bash
python scripts/deploy_parallel_ingestion.py \
    --title 7 \
    --date 2025-08-22 \
    --batch-size 20 \
    --cleanup
```

## Performance Comparison

| Method | Title 7 Parts | Estimated Time | Concurrency |
|--------|---------------|---------------|-------------|
| Sequential | 553 parts | ~30 minutes | 1 |
| Parallel (20 batch) | 553 parts | ~3-5 minutes | 20 |
| Parallel (50 batch) | 553 parts | ~2-3 minutes | 50 |

## Key Features

### 🚀 **Parallel Processing**
- Processes 20-50 parts concurrently
- Automatic batching to prevent API limits
- Configurable concurrency levels

### 📊 **Real-time Monitoring**  
- Live progress updates per part
- Success/failure tracking
- Section count reporting

### 🔧 **Auto-Management**
- Deploys Cloud Function automatically
- Handles authentication and permissions
- Cleans up resources after completion

### 📈 **Verification & Reporting**
- Compares results against eCFR API
- Generates detailed JSON reports  
- BigQuery data validation

### ⚡ **Error Handling**
- Retry logic for failed parts
- Timeout management (9 minutes per function)
- Graceful degradation

## Configuration

### Environment Variables
```yaml
# cloud_functions/ecfr_ingest_part/.env.yaml
PROJECT_ID: "lawscan"
DATASET: "ecfr_enhanced"  
TABLE: "sections_enhanced"
```

### Batch Size Tuning
- **Small (10-20)**: More stable, slower
- **Medium (30-40)**: Balanced performance  
- **Large (50+)**: Fastest, may hit limits

## Command Examples

### Ingest Multiple Titles
```bash
# Ingest titles 1, 3, 7 sequentially
for title in 1 3 7; do
    python scripts/deploy_parallel_ingestion.py --title $title --cleanup
done
```

### Verify Data Only
```bash
python scripts/deploy_parallel_ingestion.py --title 7 --verify-only
```

### Custom Date Range
```bash
python scripts/deploy_parallel_ingestion.py \
    --title 7 \
    --date 2025-08-20 \
    --batch-size 25
```

## Output & Results

### Console Output
```
🚀 Starting Parallel eCFR Ingestion
📋 Title: 7, Date: 2025-08-22, Batch Size: 20

🔍 Discovering parts for Title 7...
✅ Found 553 parts for Title 7

🚀 Deploying Cloud Function: ecfr-ingest-part
✅ Successfully deployed ecfr-ingest-part

📦 Processing batch 1: Parts 1 to 20
✅ Title 7, Part 1: 256 sections
✅ Title 7, Part 2: 93 sections
...

📊 INGESTION SUMMARY
==================================================
⏱️  Total Time: 187.3 seconds  
✅ Successful Parts: 553/553
❌ Failed Parts: 0
📄 Total Sections: 17,358
```

### JSON Report
```json
{
  "summary": {
    "title": 7,
    "date": "2025-08-22", 
    "total_time": 187.3,
    "parts_attempted": 553,
    "parts_successful": 553,
    "sections_ingested": 17358
  },
  "bigquery_stats": {
    "parts_ingested": 553,
    "sections_ingested": 17358,
    "reserved_sections": 970
  }
}
```

## Troubleshooting

### Common Issues

**1. Authentication Error**
```bash
gcloud auth login
gcloud config set project lawscan
```

**2. API Not Enabled**  
```bash
gcloud services enable cloudfunctions.googleapis.com
```

**3. Function Timeout**
- Reduce batch size: `--batch-size 10`
- Check BigQuery permissions
- Verify eCFR API availability

**4. Memory Issues**
- Function uses 2GB memory by default
- Large parts (e.g., Part 1000+) may need adjustment

### Monitoring
```bash
# View function logs
gcloud functions logs read ecfr-ingest-part --region us-central1

# Monitor BigQuery  
bq query "SELECT COUNT(*) FROM lawscan.ecfr_enhanced.sections_enhanced"
```

## Cost Optimization

### Cloud Function Costs
- **Runtime**: ~$0.0000004 per 100ms
- **Invocations**: ~$0.0000004 per request  
- **Estimated Title 7 cost**: < $0.50

### BigQuery Costs
- **Storage**: ~$0.02/GB/month
- **Queries**: ~$5/TB processed
- **Title 7 data**: ~100MB storage

## Security

- Functions run with minimal IAM permissions
- BigQuery access scoped to specific dataset
- No persistent storage of sensitive data
- Auto-cleanup removes temporary resources

## Next Steps

1. **Scale to All Titles**: Run for all 50 CFR titles
2. **Scheduled Updates**: Use Cloud Scheduler for daily updates
3. **Enhanced Monitoring**: Add Cloud Monitoring alerts
4. **Data Pipeline**: Integrate with other analytics tools

## Support

For issues or questions:
1. Check function logs: `gcloud functions logs read ecfr-ingest-part`
2. Verify API connectivity: Test eCFR API endpoints
3. Check BigQuery permissions and quotas
4. Review the orchestration script logs