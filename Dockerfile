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

# Copy git metadata first (needed for version detection)
COPY .git/ ./.git/

# Copy dependency files for better Docker layer caching
COPY pyproject.toml ./
COPY README.md ./

# Install Python dependencies (now git is available for version detection)
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -e .

# Copy source code
COPY src/ ./src/
COPY scripts/ ./scripts/

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash tellus

# Create directories with proper ownership for database and cache
RUN mkdir -p /home/tellus/.local/tellus /home/tellus/.cache/tellus && chown -R tellus:tellus /home/tellus

USER tellus

# Expose the API port
EXPOSE 1968

# Health check using the new CLI command
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f $(tellus api-info --url-only) || exit 1

# Default command - run the API server
CMD ["uvicorn", "src.tellus.interfaces.web.main:app", "--host", "0.0.0.0", "--port", "1968"]