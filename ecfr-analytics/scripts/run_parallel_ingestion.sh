#!/bin/bash
"""
Shell script to run parallel eCFR ingestion
"""

set -e

# Configuration
TITLE=${1:-7}
DATE=${2:-2025-08-22}
BATCH_SIZE=${3:-20}  # Reduced batch size for stability
PROJECT_ID=${PROJECT_ID:-lawscan}
REGION=${REGION:-us-central1}

echo "🚀 Starting Parallel eCFR Ingestion"
echo "📋 Title: $TITLE, Date: $DATE, Batch Size: $BATCH_SIZE"
echo "🌐 Project: $PROJECT_ID, Region: $REGION"
echo ""

# Check prerequisites
echo "🔍 Checking prerequisites..."

# Check if gcloud is installed and authenticated
if ! command -v gcloud &> /dev/null; then
    echo "❌ gcloud CLI not found. Please install Google Cloud SDK."
    exit 1
fi

# Check authentication
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -n1 > /dev/null; then
    echo "❌ Not authenticated with gcloud. Please run: gcloud auth login"
    exit 1
fi

# Set project
gcloud config set project $PROJECT_ID

# Enable required APIs
echo "🔧 Enabling required APIs..."
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable bigquery.googleapis.com

echo "✅ Prerequisites checked"
echo ""

# Install Python dependencies
echo "📦 Installing Python dependencies..."
pip install -q aiohttp python-dateutil google-cloud-bigquery requests

# Run the orchestration script
echo "🎯 Starting orchestration..."
python scripts/deploy_parallel_ingestion.py \
    --title $TITLE \
    --date $DATE \
    --batch-size $BATCH_SIZE \
    --cleanup

echo "🎉 Parallel ingestion complete!"