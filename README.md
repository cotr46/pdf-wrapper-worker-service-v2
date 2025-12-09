# Document Processing Worker

Worker service untuk memproses dokumen secara asynchronous menggunakan Pub/Sub dan AI models.

## Quick Start

### Manual Deployment (Recommended untuk first time)

```bash
# 1. Clone repository
git clone https://github.com/your-org/document-processing-worker.git
cd document-processing-worker

# 2. Copy your pdf_processor.py to this directory

# 3. Deploy menggunakan Cloud Build
gcloud builds submit . --config=cloudbuild.yaml

# 4. Update environment variables (IMPORTANT!)
gcloud run services update document-processing-worker \
  --region=asia-southeast2 \
  --set-env-vars="OPENWEBUI_API_KEY=your-actual-api-key,OPENWEBUI_BASE_URL=https://your-actual-endpoint.com"
```

### GitHub Actions Deployment

```bash
# Setup GitHub secrets
gh secret set WIF_PROVIDER --body "projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/github-pool/providers/github-provider"
gh secret set WIF_SERVICE_ACCOUNT --body "document-processing-sa@bni-prod-dma-bnimove-ai.iam.gserviceaccount.com"

# Push to main branch
git push origin main
# GitHub Actions will automatically deploy
```

## Environment Variables

**⚠️ IMPORTANT: Update these values before deployment!**

```bash
# Required for GCP Integration (auto-configured)
GOOGLE_CLOUD_PROJECT=bni-prod-dma-bnimove-ai
PUBSUB_SUBSCRIPTION=document-processing-request-sub
FIRESTORE_DATABASE=document-processing-firestore

# Required for AI Processing (MUST BE UPDATED!)
OPENWEBUI_API_KEY=your-openwebui-api-key        # ⚠️ UPDATE THIS!
OPENWEBUI_BASE_URL=https://your-endpoint.com    # ⚠️ UPDATE THIS!
```

## Architecture

```
[API Service] -> [Pub/Sub] -> [Worker Service] -> [AI Model]
                                    |
                              [Firestore] <- [GCS Storage]
```

## Processing Flow

1. **Receive Message** - Worker pulls dari Pub/Sub subscription
2. **Download File** - Download document dari GCS
3. **Process Document** - Menggunakan AI models via OpenWebUI
4. **Update Status** - Store result di Firestore
5. **Clean Up** - Remove temporary files

## Document Types Support

- **SKU** - Surat Keterangan Usaha
- **NPWP** - NPWP Documents
- **KTP** - Identity Cards
- **NIB** - NIB Documents
- **BPKB** - Vehicle Documents
- **SHM/SHGB** - Property Certificates

## Monitoring

```bash
# Check service status
gcloud run services describe document-processing-worker --region=asia-southeast2

# View worker logs
gcloud logs tail "resource.labels.service_name=document-processing-worker"

# Check Pub/Sub queue
gcloud pubsub subscriptions describe document-processing-request-sub
```

## Testing

```bash
# Test worker manually
gcloud pubsub topics publish document-processing-request \
  --message='{
    "job_id": "test-123",
    "document_type": "sku", 
    "gcs_path": "gs://sbp-wrapper-bucket/uploads/test/sample.pdf",
    "filename": "sample.pdf",
    "model_name": "image-screening-sku-analysis-grb"
  }'

# Check job result di Firestore atau API
curl "https://your-api-url/api/status/test-123"
```

## Troubleshooting

### Common Issues

1. **Worker tidak receive messages**
   - Check min-instances=1 di Cloud Run
   - Verify Pub/Sub subscription exists

2. **AI processing gagal**
   - Check OPENWEBUI_API_KEY dan OPENWEBUI_BASE_URL
   - Verify OpenWebUI endpoint accessibility

3. **File download gagal**
   - Check GCS bucket permissions
   - Verify service account roles

## Files Description

- `worker.py` - Main worker service code
- `pdf_processor.py` - Document processing logic (copy from existing)
- `requirements.txt` - Python dependencies
- `Dockerfile` - Container configuration
- `worker-service.yaml` - Cloud Run service config
- `cloudbuild.yaml` - Cloud Build configuration
- `.github/workflows/deploy.yml` - GitHub Actions workflow

## Security

- ✅ No external ingress (internal only)
- ✅ Non-root user execution
- ✅ Minimal IAM permissions
- ✅ Temporary file cleanup
- ✅ Secret management via environment variables
