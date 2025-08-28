# Full CFR Ingestion Guide

## Overview

The full CFR ingestion script processes all 50 CFR titles using local parallel processing with automatic verification. It's designed to handle the complete Code of Federal Regulations efficiently and reliably.

## Features

- ✅ **All 50 titles** - Processes complete CFR corpus
- ⚡ **Local parallel processing** - 5-20x speedup over sequential
- 🔍 **Automatic verification** - Compares against eCFR API counts
- 📊 **Progress tracking** - Real-time progress and ETA
- 🔄 **Resume capability** - Continue from any title
- 📁 **Detailed reporting** - Comprehensive JSON results
- ⚙️ **Smart workers** - Auto-adjusts workers based on title size

## Quick Start

### Basic Usage

```bash
# Process all 50 titles (will prompt for confirmation)
python scripts/full_cfr_ingestion.py

# Process specific titles
python scripts/full_cfr_ingestion.py --titles 1 3 5 7

# Process titles in range
python scripts/full_cfr_ingestion.py --titles-range 1 10
```

### Recommended Production Run

```bash
python scripts/full_cfr_ingestion.py \
    --max-workers 12 \
    --save-results full_cfr_$(date +%Y%m%d).json
```

### Development/Testing

```bash
# Dry run (no BigQuery insertion)
python scripts/full_cfr_ingestion.py --titles 1 3 5 --dry-run

# Skip verification for speed
python scripts/full_cfr_ingestion.py --titles 1 3 5 --no-verify
```

## Command Line Options

| Option | Description | Example |
|--------|-------------|---------|
| `--titles` | Specific titles to process | `--titles 1 3 5 7` |
| `--titles-range` | Process range of titles | `--titles-range 10 20` |
| `--date` | Version date | `--date 2025-08-22` |
| `--max-workers` | Workers per title | `--max-workers 16` |
| `--no-verify` | Skip verification | `--no-verify` |
| `--resume-from` | Resume from title | `--resume-from 25` |
| `--dry-run` | Test without BigQuery | `--dry-run` |
| `--save-results` | Save to JSON file | `--save-results results.json` |

## Performance Estimates

### Sequential vs Parallel Comparison
| Title Range | Sequential Time | Parallel Time (12 workers) | Speedup |
|------------|----------------|----------------------------|---------|
| Titles 1-10 | ~2 hours | ~15 minutes | 8x |
| Titles 1-25 | ~8 hours | ~1 hour | 8x |
| All 50 titles | ~20 hours | ~2.5 hours | 8x |

### Individual Title Performance
| Title | Parts | Sections | Sequential | Parallel (12w) | Speedup |
|-------|-------|----------|------------|----------------|---------|
| 3 | 3 | 27 | 30s | 10s | 3x |
| 5 | 284 | 1,709 | 15m | 2m | 7.5x |
| 7 | 553 | 17,358 | 47m | 6m | 7.8x |
| 12 | ~1,100 | ~25,000 | 90m | 12m | 7.5x |

## Typical Workflows

### 1. Full Production Ingestion

```bash
# Complete CFR ingestion with verification
python scripts/full_cfr_ingestion.py \
    --max-workers 12 \
    --save-results production_$(date +%Y%m%d).json \
    2>&1 | tee full_cfr.log
```

**Estimated time:** 2.5-3 hours  
**Expected output:** ~500,000+ sections across all titles

### 2. Test Subset

```bash
# Test with smaller titles first
python scripts/full_cfr_ingestion.py \
    --titles 1 2 3 4 5 \
    --dry-run \
    --save-results test_run.json
```

**Estimated time:** 5-10 minutes  
**Purpose:** Validate setup and configuration

### 3. Large Title Focus

```bash
# Process just the largest titles
python scripts/full_cfr_ingestion.py \
    --titles 7 12 40 49 \
    --max-workers 16
```

**Focus:** High-impact regulatory domains (Agriculture, Labor, Environment)

### 4. Resume After Interruption

```bash
# Continue from Title 25 after interruption
python scripts/full_cfr_ingestion.py \
    --resume-from 25 \
    --save-results resumed_$(date +%H%M).json
```

## Understanding Output

### Console Progress
```
🚀 FULL CFR INGESTION STARTING
============================================================
📅 Date: 2025-08-22
📊 Titles: 50 titles
🔧 Max Workers: 12
✅ Verification: Enabled
⏰ Started: 2025-08-28 10:00:00

============================================================
📖 PROCESSING TITLE 1 (1/50)
============================================================
📋 Title 1: General Provisions
📊 Estimated: 45 parts, 234 sections
🔧 Using 8 workers for this title

🔄 Starting ingestion...
✅ Ingestion successful: 234 sections

🔍 Running verification...
✅ Verification passed

⏱️ Title 1 completed in 45.2s

📊 PROGRESS SUMMARY:
   Completed: 1/50 titles (2.0%)
   Successful: 1
   Failed: 0
   Elapsed: 0.8 minutes
   ETA: 37.5 minutes remaining
   Total sections: 234
```

### Final Summary
```
🎉 FULL CFR INGESTION COMPLETE
============================================================
⏱️  Total Time: 156.3 minutes
📖 Titles Processed: 50
✅ Successful: 48
❌ Failed: 2
📄 Total Sections: 487,392
📦 Total Parts: 12,847
🔍 Verifications: 46/48 passed
```

## JSON Results Format

```json
{
  "started_at": "2025-08-28T10:00:00",
  "completed_at": "2025-08-28T12:36:18",
  "total_time": 9378.4,
  "titles_processed": 50,
  "titles_successful": 48,
  "titles_failed": 2,
  "total_parts_ingested": 12847,
  "total_sections_ingested": 487392,
  "verification_matches": 46,
  "verification_mismatches": 2,
  "detailed_results": [
    {
      "title": 1,
      "status": "completed",
      "title_info": {
        "number": 1,
        "name": "General Provisions",
        "reserved": false
      },
      "size_estimate": {
        "parts": 45,
        "sections": 234
      },
      "ingestion_result": {
        "title": 1,
        "parts_successful": 45,
        "sections_ingested": 234,
        "total_time": 45.2
      },
      "verification_result": {
        "overall_match": true,
        "api_counts": {"parts": 45, "sections": 234},
        "bq_counts": {"parts_ingested": 45, "sections_ingested": 234}
      },
      "processing_time": 47.8
    }
  ]
}
```

## Worker Configuration

### Auto-scaling Logic
```python
# Script automatically adjusts workers based on title size
if size_estimate['parts'] < 10:     workers = 4   # Small titles
elif size_estimate['parts'] < 50:   workers = 8   # Medium titles  
elif size_estimate['parts'] < 200:  workers = 12  # Large titles
else:                              workers = 16   # Very large titles
```

### Manual Override
```bash
# Force specific worker count for all titles
python scripts/full_cfr_ingestion.py --max-workers 8

# Good for memory-constrained systems
python scripts/full_cfr_ingestion.py --max-workers 4
```

## Error Handling

### Common Issues

**1. Memory Issues**
```bash
# Reduce workers if hitting memory limits
python scripts/full_cfr_ingestion.py --max-workers 6
```

**2. Network Timeouts**
```bash
# Resume from last successful title
python scripts/full_cfr_ingestion.py --resume-from 23
```

**3. BigQuery Permissions**
```bash
# Test with dry run first
python scripts/full_cfr_ingestion.py --dry-run --titles 1
```

### Monitoring & Recovery

**Check progress:**
```bash
# Monitor in real-time
tail -f full_cfr.log

# Check BigQuery status
bq query "SELECT title_num, COUNT(*) FROM lawscan.ecfr_enhanced.sections_enhanced GROUP BY title_num ORDER BY title_num"
```

**Resume strategies:**
```bash
# Resume from specific title
python scripts/full_cfr_ingestion.py --resume-from 30

# Re-process failed titles only
python scripts/full_cfr_ingestion.py --titles 15 23 47
```

## Advanced Usage

### Custom Date Processing
```bash
# Process historical version
python scripts/full_cfr_ingestion.py --date 2025-01-01

# Process multiple dates (run separately)
for date in 2025-08-01 2025-08-15 2025-08-22; do
  python scripts/full_cfr_ingestion.py --date $date --save-results cfr_$date.json
done
```

### Performance Tuning
```bash
# Maximum performance (if system can handle it)
python scripts/full_cfr_ingestion.py --max-workers 20

# Conservative (reliable but slower)  
python scripts/full_cfr_ingestion.py --max-workers 6

# Balance performance and API limits
python scripts/full_cfr_ingestion.py --max-workers 12  # Recommended
```

### Verification Only
```bash
# Run verification on existing data
python scripts/verify_ecfr.py --all --date 2025-08-22
```

## Integration with Other Tools

### Chain with Analysis
```bash
# Full pipeline: ingest → verify → analyze
python scripts/full_cfr_ingestion.py && \
python scripts/verify_ecfr.py --all && \
python scripts/inventory.py --stats-only
```

### Export Results
```bash
# Convert JSON results to CSV
python -c "
import json, csv
with open('full_cfr_results.json') as f: data = json.load(f)
with open('summary.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Title', 'Status', 'Parts', 'Sections', 'Time'])
    for r in data['detailed_results']:
        if r['status'] == 'completed':
            ing = r['ingestion_result']
            writer.writerow([r['title'], r['status'], ing['parts_successful'], ing['sections_ingested'], ing['total_time']])
"
```

## Expected Resource Usage

### System Requirements
- **CPU:** 8+ cores recommended (12+ optimal)
- **Memory:** 16GB+ (32GB for max workers)
- **Storage:** 10GB+ free space for BigQuery staging
- **Network:** Stable high-speed connection

### Processing Costs
- **Local compute:** Free (uses your hardware)
- **BigQuery storage:** ~$2-5/month for full CFR
- **API calls:** Free (eCFR API is public)
- **Total:** Essentially free for local processing

## Troubleshooting Guide

### Pre-flight Checks
```bash
# Test system capabilities
python -c "import multiprocessing; print(f'CPU cores: {multiprocessing.cpu_count()}')"

# Test BigQuery connection
python -c "from google.cloud import bigquery; print('BigQuery OK')"

# Test eCFR API
curl -s "https://www.ecfr.gov/api/versioner/v1/titles.json" | head -5
```

### Common Solutions
1. **Out of memory:** Reduce `--max-workers`
2. **API timeouts:** Add delays in script
3. **BigQuery errors:** Check permissions and quotas
4. **Interrupted run:** Use `--resume-from`

The full CFR ingestion script provides a robust, scalable solution for processing the entire Code of Federal Regulations with comprehensive verification and reporting.