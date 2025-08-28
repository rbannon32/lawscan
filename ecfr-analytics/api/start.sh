#!/bin/bash
set -e

echo "🚀 Starting eCFR Analytics Cloud Run Service"

# Debug: check if files exist
echo "📋 Checking files..."
ls -la /app/
echo "📋 Current directory:"
pwd

# Debug: check if Python can import main
echo "🔍 Testing import..."
python -c "import main; print('✅ Import successful')" || echo "❌ Import failed"

# Start FastAPI server in background
echo "🔧 Starting FastAPI server..."
uvicorn main:app --host 127.0.0.1 --port 8000 --workers 1 --log-level debug &
FASTAPI_PID=$!

# Wait a moment for API to start and check if it's still running
sleep 5

# Check if FastAPI is still running
if kill -0 $FASTAPI_PID 2>/dev/null; then
    echo "✅ FastAPI server is running (PID: $FASTAPI_PID)"
else
    echo "❌ FastAPI server failed to start or crashed"
    exit 1
fi

# Start nginx in foreground (this keeps the container running)
echo "📄 Starting nginx for UI..."
exec nginx -g "daemon off;"