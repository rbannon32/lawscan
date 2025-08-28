#!/bin/bash
set -e

# eCFR Analytics - Cloud Run Deployment Script
echo "🚀 Deploying eCFR Analytics to Cloud Run"

# Configuration (edit these values)
PROJECT_ID="${PROJECT_ID:-lawscan}"
SERVICE_NAME="${SERVICE_NAME:-ecfr-analytics}"
REGION="${REGION:-us-central1}"
GEMINI_API_KEY="${GEMINI_API_KEY}"

if [ -z "$GEMINI_API_KEY" ]; then
    echo "❌ Error: GEMINI_API_KEY environment variable is required"
    echo "   Get your API key from: https://makersuite.google.com/app/apikey"
    echo "   Then run: export GEMINI_API_KEY='your-api-key-here'"
    exit 1
fi

echo "📋 Configuration:"
echo "   Project ID: $PROJECT_ID"
echo "   Service: $SERVICE_NAME"
echo "   Region: $REGION"
echo ""

# Set the project
echo "🔧 Setting GCP project..."
gcloud config set project $PROJECT_ID

# Enable required services
echo "🔧 Enabling required Google Cloud services..."
gcloud services enable \
    cloudbuild.googleapis.com \
    run.googleapis.com \
    bigquery.googleapis.com \
    aiplatform.googleapis.com

# Build and deploy
echo "🏗️  Building container image..."
gcloud builds submit --tag gcr.io/$PROJECT_ID/$SERVICE_NAME .

echo "🚀 Deploying to Cloud Run..."
gcloud run deploy $SERVICE_NAME \
    --image gcr.io/$PROJECT_ID/$SERVICE_NAME \
    --platform managed \
    --region $REGION \
    --allow-unauthenticated \
    --memory=2Gi \
    --cpu=1 \
    --max-instances=10 \
    --set-env-vars="PROJECT_ID=$PROJECT_ID,DATASET=ecfr_enhanced,TABLE=sections_enhanced,GEMINI_API_KEY=$GEMINI_API_KEY" \
    --port=8080

# Get the service URL
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --platform managed --region $REGION --format 'value(status.url)')

echo ""
echo "🎉 Deployment complete!"
echo "📱 Service URL: $SERVICE_URL"
echo ""
echo "🔧 Next steps:"
echo "1. Ensure your BigQuery dataset 'ecfr_enhanced' contains data"
echo "2. Test the deployment: curl $SERVICE_URL/health"
echo "3. Access the UI: $SERVICE_URL"
echo ""
echo "💡 To update environment variables:"
echo "   gcloud run services update $SERVICE_NAME --region $REGION --set-env-vars=\"GEMINI_API_KEY=new-key\""