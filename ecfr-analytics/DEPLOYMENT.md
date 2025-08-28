# eCFR Analytics - Cloud Run Deployment Guide

This guide documents the complete process for deploying the eCFR Analytics application to Google Cloud Run as a single, publicly accessible service combining both frontend (UI) and backend (API).

## 🏗️ Architecture Overview

The deployment creates a single Cloud Run service that runs:
- **nginx** (port 8080) - serves the frontend UI and proxies API requests
- **FastAPI** (port 8000) - handles API endpoints and BigQuery/Gemini AI integration

## 📋 Prerequisites

### Required Tools
- Google Cloud SDK (`gcloud`) installed and authenticated
- Docker (for local testing, optional)
- Access to a Google Cloud Project with billing enabled

### Required APIs
The deployment script automatically enables these, but you can enable manually:
```bash
gcloud services enable \
  cloudbuild.googleapis.com \
  run.googleapis.com \
  bigquery.googleapis.com \
  aiplatform.googleapis.com
```

### Required Permissions
Your account needs these IAM roles:
- `roles/cloudbuild.builds.editor`
- `roles/run.admin` 
- `roles/iam.serviceAccountUser`
- `roles/serviceusage.serviceUsageAdmin`

## 🔑 Environment Setup

### 1. Get API Keys
```bash
# Get your Gemini API key from: https://makersuite.google.com/app/apikey
export GEMINI_API_KEY="your-gemini-api-key-here"
```

### 2. Set Project Configuration
```bash
export PROJECT_ID="your-project-id"
export SERVICE_NAME="ecfr-analytics" 
export REGION="us-central1"
```

## 🚀 Deployment Process

### Option A: One-Click Deployment (Recommended)
```bash
# Make deployment script executable
chmod +x deploy-cloud-run.sh

# Set your API key and deploy
export GEMINI_API_KEY="your-api-key-here"
./deploy-cloud-run.sh
```

### Option B: Manual Step-by-Step

#### 1. Build Container
```bash
gcloud builds submit --tag gcr.io/$PROJECT_ID/ecfr-analytics .
```

#### 2. Deploy to Cloud Run
```bash
gcloud run deploy ecfr-analytics \
  --image gcr.io/$PROJECT_ID/ecfr-analytics \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --memory=2Gi \
  --cpu=1 \
  --max-instances=10 \
  --set-env-vars="PROJECT_ID=$PROJECT_ID,DATASET=ecfr_enhanced,TABLE=sections_enhanced,GEMINI_API_KEY=$GEMINI_API_KEY" \
  --port=8080
```

## 🔧 Container Architecture

### Dockerfile Structure
The deployment uses the **root-level Dockerfile** (not `/api/Dockerfile`) which:

1. **Base Image**: `python:3.11-slim`
2. **System Dependencies**: nginx for reverse proxy
3. **Python Dependencies**: FastAPI, BigQuery, Gemini AI libraries
4. **File Layout**:
   - API code: `/app/main.py`
   - UI files: `/var/www/html/`
   - nginx config: `/etc/nginx/sites-available/default`
   - Startup script: `/app/start.sh`

### Service Startup Process
The `start.sh` script:
1. Validates environment and file permissions
2. Starts FastAPI server on `127.0.0.1:8000` (background)
3. Starts nginx on port `8080` (foreground, keeps container alive)

### nginx Configuration
```nginx
server {
    listen 8080;
    
    # Serve UI files
    location / {
        root /var/www/html;
        index index.html;
        try_files $uri $uri/ /index.html;
    }
    
    # Proxy API requests
    location /api/ {
        proxy_pass http://127.0.0.1:8000;
        # ... proxy headers and timeouts
    }
    
    # Health check
    location /health {
        proxy_pass http://127.0.0.1:8000/healthz;
    }
}
```

## ⚠️ Common Pitfalls & Solutions

### 1. Docker Build Context Issues
**Problem**: `COPY failed: forbidden path outside the build context`
**Cause**: Building from wrong directory or using wrong Dockerfile
**Solution**: 
- Always build from project root: `gcloud builds submit --tag gcr.io/$PROJECT_ID/$SERVICE_NAME .`
- Remove conflicting `api/cloudbuild.yaml` files
- Use root-level `Dockerfile`, not `api/Dockerfile`

### 2. Permission Denied (Cloud Build)
**Problem**: `PERMISSION_DENIED: The caller does not have permission`
**Solution**:
```bash
# Set quota project
gcloud auth application-default set-quota-project $PROJECT_ID

# Add IAM permissions for Cloud Build service account
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$(gcloud projects describe $PROJECT_ID --format='value(projectNumber)')@cloudbuild.gserviceaccount.com" \
  --role="roles/run.admin"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$(gcloud projects describe $PROJECT_ID --format='value(projectNumber)')@cloudbuild.gserviceaccount.com" \
  --role="roles/iam.serviceAccountUser"
```

### 3. UI Files Not Found (403 Forbidden)
**Problem**: nginx returns 403 Forbidden for UI files
**Causes & Solutions**:
- **Missing files**: UI directory not copied to container
  - Fix: Ensure `COPY ui/ /var/www/html/` in Dockerfile
- **Wrong permissions**: nginx can't read files
  - Fix: Add `RUN chmod -R 644 /var/www/html/ && chmod 755 /var/www/html/`
- **Wrong Dockerfile**: Using api/Dockerfile instead of root Dockerfile
  - Fix: Remove `api/cloudbuild.yaml`, build from project root

### 4. API Server Crashes
**Problem**: FastAPI server starts but crashes immediately
**Causes & Solutions**:
- **Missing dependencies**: Import errors
  - Fix: Check `requirements.txt` includes all dependencies
- **Environment variables**: Missing required env vars
  - Fix: Verify all required env vars are set in deployment
- **BigQuery permissions**: Service account can't access BigQuery
  - Fix: Ensure Cloud Run service account has BigQuery permissions

### 5. Process Check Failures
**Problem**: Container startup fails with process check errors
**Cause**: Using `ps` command which isn't available in slim containers
**Solution**: Use `kill -0 $PID` instead of `ps -p $PID` for process checks

### 6. File Exclusion (.dockerignore/.gcloudignore)
**Problem**: Required files not included in build context
**Solution**: Check `.dockerignore` and `.gcloudignore` files:
```
# Ensure these lines are present
!api/
!ui/
!api/**
!ui/**
```

## 🧪 Testing Deployment

### 1. Health Checks
```bash
SERVICE_URL="https://your-service-url"

# Test API health
curl $SERVICE_URL/health

# Test UI loading  
curl -s $SERVICE_URL/ | head -5

# Test API endpoint
curl "$SERVICE_URL/api/agency/wordcount?date=2024-01-01"
```

### 2. Verify Public Access
```bash
# Check IAM policy - should show allUsers with roles/run.invoker
gcloud run services get-iam-policy ecfr-analytics --region=us-central1
```

### 3. Monitor Logs
```bash
# Check recent logs
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=ecfr-analytics" --limit=20 --project=$PROJECT_ID

# Check for errors
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=ecfr-analytics AND severity>=ERROR" --limit=10 --project=$PROJECT_ID
```

## 🔄 Updates & Redeployment

### Code Updates
```bash
# For code changes, rebuild and deploy:
export GEMINI_API_KEY="your-api-key"
gcloud builds submit --tag gcr.io/$PROJECT_ID/ecfr-analytics .
gcloud run deploy ecfr-analytics --image gcr.io/$PROJECT_ID/ecfr-analytics --region=us-central1
```

### Environment Variable Updates
```bash
# Update environment variables without rebuilding:
gcloud run services update ecfr-analytics \
  --region=us-central1 \
  --set-env-vars="NEW_VAR=new-value,EXISTING_VAR=updated-value"
```

## 📊 Resource Configuration

### Current Settings
- **Memory**: 2GB (adjustable based on usage)
- **CPU**: 1 vCPU (can be fractional like 0.5)
- **Max Instances**: 10 (adjust based on expected traffic)
- **Timeout**: 300s for AI analysis requests
- **Port**: 8080 (nginx frontend/proxy)

### Scaling Considerations
- **Memory**: Increase if BigQuery operations are large
- **CPU**: Increase for CPU-intensive AI processing
- **Max Instances**: Set based on expected concurrent users
- **Request Timeout**: Increase for long-running AI analysis

## 🔐 Security Notes

### Environment Variables
- Gemini API key is passed as environment variable (secure in Cloud Run)
- BigQuery access uses default service account credentials
- No secrets are logged or exposed in container logs

### Network Security
- Service runs on private Google network
- Only port 8080 exposed externally
- nginx handles all external traffic, FastAPI only accessible internally

### Access Control
- Currently configured for public access (`--allow-unauthenticated`)
- To restrict access, remove `--allow-unauthenticated` and configure IAM

## 📚 Additional Resources

- [Cloud Run Documentation](https://cloud.google.com/run/docs)
- [BigQuery Client Libraries](https://cloud.google.com/bigquery/docs/reference/libraries)
- [Gemini API Documentation](https://ai.google.dev/docs)
- [nginx Configuration Guide](https://nginx.org/en/docs/)

## 🐛 Troubleshooting Commands

```bash
# Check service status
gcloud run services describe ecfr-analytics --region=us-central1

# View recent revisions
gcloud run revisions list --service=ecfr-analytics --region=us-central1

# Check build history
gcloud builds list --limit=5

# Test local container (if built locally)
docker run -p 8080:8080 -e PROJECT_ID=$PROJECT_ID -e GEMINI_API_KEY=$GEMINI_API_KEY gcr.io/$PROJECT_ID/ecfr-analytics
```