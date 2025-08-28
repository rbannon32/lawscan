# eCFR Bulk XML Ingestion

## Overview

This system downloads the complete Code of Federal Regulations (CFR) as XML files from the eCFR bulk data API and stores them in Google Cloud Storage for processing. This approach avoids API rate limits by using publicly available bulk XML exports.

## Structure

```
bulk-ingestion/
├── scripts/
│   ├── download_cfr_xml.py    # Downloads XML files and uploads to GCS
│   ├── parse_xml_to_bq.py     # Parses XML and loads to BigQuery (coming next)
│   └── validate_ingestion.py  # Validates the ingestion (coming next)
├── data/                       # Local XML file storage (temporary)
├── logs/                       # Download and processing logs
└── README.md
```

## GCS Bucket

- **Bucket**: `gs://ecfr-xml-bulk-2025`
- **Structure**: `{date}/title-{number}.xml`
- **Example**: `gs://ecfr-xml-bulk-2025/2025-08-22/title-01.xml`

## Usage

### Download Specific Titles
```bash
# Download titles 1, 2, and 3
python scripts/download_cfr_xml.py --titles 1 2 3

# Download titles 1-10
python scripts/download_cfr_xml.py --range 1 10

# Download all 50 titles
python scripts/download_cfr_xml.py --range 1 50
```

### Options
- `--no-upload`: Download only, don't upload to GCS
- `--date YYYY-MM-DD`: Specify version date (default: 2025-08-22)

### Test Run (Recommended First)
```bash
# Test with just title 3 (smallest)
cd bulk-ingestion
python scripts/download_cfr_xml.py --titles 3
```

## XML File Sizes (Estimated)

| Title | Subject | Est. Size |
|-------|---------|-----------|
| 3 | The President | ~1 MB |
| 1 | General Provisions | ~5 MB |
| 5 | Administrative Personnel | ~50 MB |
| 7 | Agriculture | ~200 MB |
| 12 | Banks and Banking | ~300 MB |
| 26 | Internal Revenue | ~500 MB |
| 40 | Protection of Environment | ~800 MB |

**Total for all 50 titles**: ~5-10 GB

## Process Flow

1. **Download XML**: Fetch from eCFR bulk data endpoint
2. **Local Storage**: Save temporarily to `data/` folder
3. **Upload to GCS**: Transfer to Cloud Storage bucket
4. **Parse XML**: Extract sections, parts, and metadata
5. **Load to BigQuery**: Insert into `sections_enhanced` table
6. **Cleanup**: Delete local XML files after processing

## URLs

GovInfo provides bulk XML exports at:
```
https://www.govinfo.gov/bulkdata/ECFR/title-{number}/ECFR-title{number}.xml
```

Examples:
```
https://www.govinfo.gov/bulkdata/ECFR/title-1/ECFR-title1.xml
https://www.govinfo.gov/bulkdata/ECFR/title-49/ECFR-title49.xml
https://www.govinfo.gov/bulkdata/ECFR/title-50/ECFR-title50.xml
```

## Benefits Over API Approach

1. **No Rate Limits**: Bulk files are publicly available
2. **Complete Data**: Get entire titles at once
3. **Faster**: Single request per title vs thousands of API calls
4. **Reliable**: No timeout issues or partial data
5. **Cacheable**: Store in GCS for re-processing

## Next Steps

After downloading XML files:
1. Parse XML structure to extract sections
2. Apply agency name mappings
3. Calculate regulatory metrics
4. Load to BigQuery with proper schema
5. Validate against existing data

## Monitoring

Check download progress:
```bash
tail -f logs/download_xml.log
```

Check GCS uploads:
```bash
gsutil ls -l gs://ecfr-xml-bulk-2025/2025-08-22/
```

## Cost Estimates

- **GCS Storage**: ~$0.02/GB/month = ~$0.20/month for 10GB
- **Network Egress**: Free (downloading to GCP)
- **Operations**: Minimal (< $0.01)

Total: **< $1/month** for complete CFR storage