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
COPY requirements.txt ./
COPY Makefile ./

# Install dependencies via Makefile target
RUN make deps

# Copy application code
COPY app/ ./app/
COPY run.py ./run.py
COPY test_client.py ./test_client.py

# Expose port
EXPOSE 8000

# Run the application via Makefile target
CMD ["make", "serve"]
