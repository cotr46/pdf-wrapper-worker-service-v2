# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies needed for PDF processing
RUN apt-get update && apt-get install -y \
    build-essential \
    libffi-dev \
    libjpeg-dev \
    libpng-dev \
    libfreetype6-dev \
    liblcms2-dev \
    libopenjp2-7-dev \
    libtiff5-dev \
    libwebp-dev \
    tcl8.6-dev \
    tk8.6-dev \
    python3-tk \
    libharfbuzz-dev \
    libfribidi-dev \
    libxcb1-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better Docker layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY worker.py .
COPY pdf_processor.py .

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash worker \
    && chown -R worker:worker /app

# Switch to non-root user
USER worker

# Health check - check if worker can import dependencies
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import google.cloud.pubsub_v1, google.cloud.storage, google.cloud.firestore; print('OK')" || exit 1

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV GOOGLE_CLOUD_PROJECT=bni-prod-dma-bnimove-ai

# Run the worker
CMD ["python", "worker.py"]
