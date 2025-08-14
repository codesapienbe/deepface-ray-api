FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    libgomp1 \
    ffmpeg \
    curl \
    make \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Ensure uv is on PATH
ENV PATH="/root/.local/bin:${PATH}"

# Performance/threads: keep CPU/lightweight
ENV OMP_NUM_THREADS=1 \
    OPENBLAS_NUM_THREADS=1 \
    MKL_NUM_THREADS=1 \
    TF_NUM_INTEROP_THREADS=1 \
    TF_NUM_INTRAOP_THREADS=1 \
    RAY_DISABLE_DASHBOARD=1 \
    RAY_memory_usage_threshold=0.98

# Install uv
RUN curl -fsSL https://astral.sh/uv/install.sh | sh

# Copy dependency files and Makefile
COPY pyproject.toml ./

# Install dependencies
RUN uv pip install --system --no-cache-dir .

# Copy application code
COPY app/ ./app/
COPY run.py ./run.py

# Create entrypoint script to start API (Ray runs in-process local mode)
RUN echo '#!/usr/bin/env bash' > /app/entrypoint.sh && \
    echo 'set -euo pipefail' >> /app/entrypoint.sh && \
    echo '' >> /app/entrypoint.sh && \
    echo '# Prepare directories' >> /app/entrypoint.sh && \
    echo 'mkdir -p /tmp/ray/spill || true' >> /app/entrypoint.sh && \
    echo '' >> /app/entrypoint.sh && \
    echo '# Ensure API sees auto address; app will fall back to local mode if needed' >> /app/entrypoint.sh && \
    echo 'export RAY_ADDRESS=${RAY_ADDRESS:-auto}' >> /app/entrypoint.sh && \
    echo 'export NUM_WORKERS=${NUM_WORKERS:-1}' >> /app/entrypoint.sh && \
    echo '' >> /app/entrypoint.sh && \
    echo '# Start API' >> /app/entrypoint.sh && \
    echo 'exec python run.py --workers ${NUM_WORKERS} --host 0.0.0.0 --port 8000' >> /app/entrypoint.sh && \
    chmod +x /app/entrypoint.sh

# Expose port
EXPOSE 8000

# Environment defaults
ENV RAY_object_store_memory=268435456 \
    RAY_spill_dir=/tmp/ray/spill \
    RAY_enable_object_reconstruction=1 \
    NUM_WORKERS=1 \
    RAY_ADDRESS=auto

# Set entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]
