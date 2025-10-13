#!/bin/bash
# Setup Nginx configuration by replacing environment variables
# This script processes the nginx config template and replaces ${DOMAIN} with actual domain

set -e

# Load environment variables from .env if it exists
if [ -f .env ]; then
    # Export variables from .env, ignoring comments and empty lines
    set -a
    source <(grep -v '^#' .env | grep -v '^$')
    set +a
else
    echo "Warning: .env file not found. Using default values."
fi

# Set defaults if not provided
DOMAIN=${DOMAIN:-api.your-domain.com}

echo "Configuring nginx with domain: $DOMAIN"

# Check if template exists
if [ ! -f nginx/conf.d/tellus-api.conf.template ]; then
    echo "Error: nginx/conf.d/tellus-api.conf.template not found"
    exit 1
fi

# Process template and create actual config
envsubst '${DOMAIN}' < nginx/conf.d/tellus-api.conf.template > nginx/conf.d/tellus-api.conf

echo "Nginx configuration created at nginx/conf.d/tellus-api.conf"

# Validate nginx configuration if docker is available
if command -v docker &> /dev/null; then
    echo "Validating nginx configuration..."
    if docker run --rm -v "$(pwd)/nginx/nginx.conf:/etc/nginx/nginx.conf:ro" \
                      -v "$(pwd)/nginx/conf.d:/etc/nginx/conf.d:ro" \
                      nginx:alpine nginx -t 2>&1 | grep -q "successful"; then
        echo "Nginx configuration is valid"
    else
        echo "Warning: Nginx configuration validation failed. Check syntax."
    fi
fi

echo "Done! Domain configured: $DOMAIN"
