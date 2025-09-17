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

# Configure git safe directory to avoid ownership issues
RUN git config --global --add safe.directory /app

# Copy dependency files for better Docker layer caching
COPY pyproject.toml ./
COPY README.md ./

# Copy source code (needed before installing the package)
COPY src/ ./src/
COPY scripts/ ./scripts/

# Install Python dependencies (now git is available for version detection)
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -e .

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash tellus

# Create directories with proper ownership for database, cache, and app state
RUN mkdir -p /home/tellus/.local/tellus /home/tellus/.cache/tellus /home/tellus/.tellus && chown -R tellus:tellus /home/tellus

# Give tellus user ownership of the app directory
RUN chown -R tellus:tellus /app

USER tellus

# Set working directory to user's home directory where .tellus will be created
WORKDIR /home/tellus

# Expose the API port
EXPOSE 1968

# Health check using the new CLI command
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f $(tellus api-info --url-only) || exit 1

# Default command - run the API server (using module path that works from /home/tellus)
CMD ["uvicorn", "tellus.interfaces.web.main:app", "--host", "0.0.0.0", "--port", "1968"]