# Sabot Dockerfile
# Build a containerized Sabot installation

FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/app
ENV SABOT_BROKER=memory://

# Install system dependencies for Cython compilation
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
COPY pyproject.toml .
COPY setup.py .
COPY MANIFEST.in .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY sabot/ ./sabot/

# Install Sabot in development mode
RUN pip install -e .

# Create a non-root user
RUN useradd --create-home --shell /bin/bash sabot
USER sabot

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD sabot status || exit 1

# Default command
CMD ["sabot", "--help"]
