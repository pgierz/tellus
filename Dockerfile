# Use Python 3.11 slim image for smaller size
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies for SSH and other tools
RUN apt-get update && apt-get install -y \
    openssh-client \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files first for better Docker layer caching
COPY pyproject.toml ./
COPY README.md ./

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -e .

# Copy source code and git metadata (needed for version detection)
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY .git/ ./.git/

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash tellus
USER tellus

# Create directories for data persistence
RUN mkdir -p /home/tellus/.cache/tellus /home/tellus/data

# Expose the API port
EXPOSE 1968

# Health check using the new CLI command
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f $(tellus api-info --url-only) || exit 1

# Default command - run the API server
CMD ["uvicorn", "src.tellus.interfaces.web.main:app", "--host", "0.0.0.0", "--port", "1968"]