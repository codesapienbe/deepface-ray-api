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

# Install uv
RUN curl -fsSL https://astral.sh/uv/install.sh | sh

# Copy dependency files and Makefile
COPY pyproject.toml ./

# Install dependencies
RUN uv pip install --system --no-cache-dir .

# Copy application code
COPY app/ ./app/
COPY run.py ./run.py

# Create entrypoint script to start Ray and API
RUN printf '#!/usr/bin/env bash\n\nset -euo pipefail\n\n# Prepare Ray directories\nmkdir -p /tmp/ray/spill || true\n\n# Start Ray head in background\nray start --head --disable-usage-stats --dashboard-host=0.0.0.0 --block &\nRAY_PID=$!\n\n# Wait briefly for Ray to be ready\nsleep 2\n\n# Ensure API sees auto address\nexport RAY_ADDRESS=auto\nexport NUM_WORKERS=${NUM_WORKERS:-1}\n\n# Start API (will connect to local Ray)\nexec python run.py --workers ${NUM_WORKERS} --host 0.0.0.0 --port 8000\n' > /app/entrypoint.sh \
 && chmod +x /app/entrypoint.sh

# Expose port
EXPOSE 8000

# Environment defaults
ENV RAY_object_store_memory=1073741824 \
    RAY_spill_dir=/tmp/ray/spill \
    RAY_enable_object_reconstruction=1 \
    NUM_WORKERS=1 \
    RAY_ADDRESS=auto

# Set entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]
