# eCFR Analytics - Cloud Run Deployment Guide

Deploy your eCFR Analytics application to Google Cloud Run as a single, cost-effective service that combines the UI and API.

## 🎯 Overview

This deployment packages:
- **FastAPI Backend** - All /api/* endpoints with Gemini AI analysis
- **Static Frontend** - HTML/CSS/JavaScript UI served via nginx
- **Single Container** - Combined service on Cloud Run (port 8080)
- **Environment Variables** - BigQuery and Gemini API configuration

## 📋 Prerequisites

1. **Google Cloud Project** with billing enabled
2. **gcloud CLI** installed and authenticated
3. **Gemini API Key** from [Google AI Studio](https://makersuite.google.com/app/apikey)
4. **BigQuery Dataset** with eCFR data (see main README for ingestion)

## 🚀 Quick Deployment

### 1. Get your Gemini API Key
```bash
# Visit https://makersuite.google.com/app/apikey
# Create a new API key and copy it
export GEMINI_API_KEY="your_api_key_here"
```

### 2. Set your GCP Project
```bash
export PROJECT_ID="your-gcp-project-id"
gcloud config set project $PROJECT_ID
```

### 3. Deploy to Cloud Run
```bash
# Run the deployment script
./deploy-cloud-run.sh

# Or deploy manually:
cd api
gcloud builds submit --tag gcr.io/$PROJECT_ID/ecfr-analytics .
gcloud run deploy ecfr-analytics \
  --image gcr.io/$PROJECT_ID/ecfr-analytics \
  --region us-central1 \
  --allow-unauthenticated \
  --memory 2Gi \
  --set-env-vars="PROJECT_ID=$PROJECT_ID,DATASET=ecfr_enhanced,TABLE=sections_enhanced,GEMINI_API_KEY=$GEMINI_API_KEY"
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `PROJECT_ID` | Your GCP project ID | `lawscan` |
| `DATASET` | BigQuery dataset name | `ecfr_enhanced` |
| `TABLE` | BigQuery table name | `sections_enhanced` |
| `GEMINI_API_KEY` | Google AI API key | `AIza...` |

### Update Environment Variables
```bash
gcloud run services update ecfr-analytics \
  --region us-central1 \
  --set-env-vars="GEMINI_API_KEY=new-api-key"
```

## 🏗️ Architecture

```
┌─────────────────┐
│   Cloud Run     │
│   (Port 8080)   │
├─────────────────┤
│ nginx (Frontend)│ ──► Serves static HTML/JS/CSS
│      ↓ proxy    │
│ FastAPI (8000)  │ ──► Handles /api/* requests
└─────────────────┘
         ↓
    BigQuery API
         ↓
    Gemini API
```

### Service Components

1. **nginx** - Serves UI files and proxies /api/* to FastAPI
2. **FastAPI** - Handles API endpoints and AI analysis
3. **Combined Container** - Single deployment, cost-effective

## 🔍 Monitoring & Troubleshooting

### Check Service Status
```bash
# Get service URL
gcloud run services describe ecfr-analytics --region us-central1 --format="value(status.url)"

# Test health endpoint
curl https://your-service-url/health

# View logs
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=ecfr-analytics" --limit 50
```

### Common Issues

**1. Build Failures**
```bash
# Check Cloud Build logs
gcloud builds list --limit=5

# View specific build
gcloud builds describe BUILD-ID
```

**2. API Errors**
```bash
# Check environment variables
gcloud run services describe ecfr-analytics --region us-central1 --format="value(spec.template.spec.template.spec.containers[0].env)"

# Test BigQuery connection
curl "https://your-service-url/api/agencies?date=2025-08-28"
```

**3. AI Analysis Issues**
```bash
# Test Gemini API key
curl -X POST "https://your-service-url/api/ai/analyze-section" \
  -H "Content-Type: application/json" \
  -d '{"section_citation":"48 CFR § 32.204","title":"48","part":"32","date":"2025-08-28"}'
```

## 💰 Cost Optimization

### Pricing (Estimated)
- **Cloud Run**: ~$5-15/month (depends on usage)
- **Container Registry**: ~$0.10/month (storage)  
- **BigQuery**: ~$2-5/month (storage + queries)
- **Total**: ~$7-20/month

### Cost Controls
```bash
# Set maximum instances to control costs
gcloud run services update ecfr-analytics \
  --max-instances=5 \
  --concurrency=100

# Set CPU allocation (cost vs performance)
gcloud run services update ecfr-analytics \
  --cpu=1 \
  --memory=1Gi  # Reduce if not using AI analysis heavily
```

## 🔐 Security

### Service Account (Recommended)
```bash
# Create service account for Cloud Run
gcloud iam service-accounts create ecfr-analytics-sa \
  --display-name="eCFR Analytics Service Account"

# Grant BigQuery permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:ecfr-analytics-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.user"

# Update Cloud Run to use service account
gcloud run services update ecfr-analytics \
  --service-account="ecfr-analytics-sa@$PROJECT_ID.iam.gserviceaccount.com"
```

### Domain & HTTPS
```bash
# Map custom domain (optional)
gcloud run domain-mappings create \
  --service ecfr-analytics \
  --domain your-domain.com \
  --region us-central1
```

## 🔄 Updates & Maintenance

### Deploy Updates
```bash
# Rebuild and deploy
cd api
gcloud builds submit --tag gcr.io/$PROJECT_ID/ecfr-analytics .
gcloud run deploy ecfr-analytics --image gcr.io/$PROJECT_ID/ecfr-analytics --region us-central1

# Or use the deployment script
./deploy-cloud-run.sh
```

### Automated Deployments
```bash
# Set up Cloud Build trigger (optional)
gcloud builds triggers create github \
  --repo-name=ecfr-analytics \
  --repo-owner=your-username \
  --branch-pattern="^main$" \
  --build-config=api/cloudbuild.yaml
```

## 📊 Performance Tuning

### Optimize for Usage Patterns
```bash
# High-traffic configuration
gcloud run services update ecfr-analytics \
  --memory=4Gi \
  --cpu=2 \
  --max-instances=20 \
  --concurrency=1000

# Cost-optimized configuration  
gcloud run services update ecfr-analytics \
  --memory=1Gi \
  --cpu=1 \
  --max-instances=3 \
  --concurrency=80
```

### Monitor Performance
- **Cloud Run Metrics** - View in Cloud Console
- **Application Logs** - Use Cloud Logging
- **BigQuery Performance** - Monitor query costs in BigQuery console

## 🎉 Success!

Your eCFR Analytics is now running on Cloud Run! The service provides:

- ✅ **Complete UI** - Browse regulations with search and AI analysis
- ✅ **Fast API** - All endpoints including Gemini AI analysis
- ✅ **Auto-scaling** - Handles traffic spikes automatically
- ✅ **HTTPS** - Secure by default
- ✅ **Cost-effective** - Pay only for actual usage

**Next Steps:**
1. Test all functionality with your live service
2. Set up monitoring and alerting
3. Consider adding a custom domain
4. Plan for regular data updates