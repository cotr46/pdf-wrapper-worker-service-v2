#!/bin/bash

# Quick Deploy Script untuk Document Processing Worker
# Untuk manual deployment tanpa GitHub Actions

set -e

PROJECT_ID="bni-prod-dma-bnimove-ai"
REGION="asia-southeast2"
SERVICE_NAME="document-processing-worker"

echo "🚀 Deploying Document Processing Worker"
echo "======================================="

# Check if gcloud is authenticated
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q "@"; then
  echo "❌ Please authenticate with gcloud first:"
  echo "  gcloud auth login"
  exit 1
fi

# Set project
echo "📝 Setting project to: $PROJECT_ID"
gcloud config set project $PROJECT_ID

# Check if pdf_processor.py exists
if [ ! -f "pdf_processor.py" ]; then
  echo "❌ pdf_processor.py not found!"
  echo "Please copy your existing pdf_processor.py to this directory"
  exit 1
fi

echo "✅ Found pdf_processor.py"

# Deploy using Cloud Build
echo "🔨 Building and deploying using Cloud Build..."
gcloud builds submit . \
  --config=cloudbuild.yaml \
  --timeout=1200s

# Get service URL
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME \
  --region=$REGION \
  --format="value(status.url)" 2>/dev/null || echo "Not deployed")

echo ""
echo "✅ Deployment completed!"
echo "========================"
echo "Service: $SERVICE_NAME"
echo "Region: $REGION"
echo "URL: $SERVICE_URL"

echo ""
echo "⚠️  IMPORTANT: Update environment variables!"
echo "============================================"
echo "Run these commands to update OpenWebUI configuration:"
echo ""
echo "gcloud run services update $SERVICE_NAME \\"
echo "  --region=$REGION \\"
echo "  --set-env-vars=\"OPENWEBUI_API_KEY=your-actual-api-key,OPENWEBUI_BASE_URL=https://your-actual-endpoint.com\""

echo ""
echo "🔍 Check deployment status:"
echo "gcloud run services describe $SERVICE_NAME --region=$REGION"

echo ""
echo "📊 Monitor logs:"
echo "gcloud logs tail \"resource.labels.service_name=$SERVICE_NAME\""

echo ""
echo "🧪 Test the worker:"
echo "gcloud pubsub topics publish document-processing-request \\"
echo "  --message='{\"job_id\":\"test-123\",\"document_type\":\"sku\",\"gcs_path\":\"gs://bucket/file.pdf\",\"filename\":\"test.pdf\"}'"
