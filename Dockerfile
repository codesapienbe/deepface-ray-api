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
    echo 'export NUM_WORKERS=${NUM_WORKERS:-2}' >> /app/entrypoint.sh && \
    echo '' >> /app/entrypoint.sh && \
    echo '# Start API' >> /app/entrypoint.sh && \
    echo 'exec python run.py --workers ${NUM_WORKERS} --host 0.0.0.0 --port 8000' >> /app/entrypoint.sh && \
    chmod +x /app/entrypoint.sh

# Expose port
EXPOSE 8000

# Environment defaults (override at runtime with -e WORKER_PROVIDER=...)
ENV RAY_object_store_memory=4294967296 \
    RAY_spill_dir=/tmp/ray/spill \
    RAY_enable_object_reconstruction=1 \
    NUM_WORKERS=1 \
    RAY_ADDRESS=auto \
    WORKER_PROVIDER=ray


# Download model weights
RUN mkdir -p /root/.deepface/weights

RUN mkdir -p /root/.deepface/weights \
    && curl -L "https://github.com/serengil/deepface_models/releases/download/v1.0/arcface_weights.h5" \
        -o /root/.deepface/weights/arcface_weights.h5 \
    && curl -L "https://github.com/serengil/deepface_models/releases/download/v1.0/retinaface.h5" \
        -o /root/.deepface/weights/retinaface.h5 \
    && curl -L "https://github.com/serengil/deepface_models/releases/download/v1.0/facial_expression_model_weights.h5" \
        -o /root/.deepface/weights/facial_expression_model_weights.h5 \
    && curl -L "https://github.com/serengil/deepface_models/releases/download/v1.0/age_model_weights.h5" \
        -o /root/.deepface/weights/age_model_weights.h5 \
    && curl -L "https://github.com/serengil/deepface_models/releases/download/v1.0/gender_model_weights.h5" \
        -o /root/.deepface/weights/gender_model_weights.h5 \
    && curl -L "https://github.com/serengil/deepface_models/releases/download/v1.0/race_model_single_batch.h5" \
        -o /root/.deepface/weights/race_model_single_batch.h5


RUN mkdir -p /opt/ttn/ttn_tmp && \
    chmod 777 /opt/ttn/ttn_tmp

# Set entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]
