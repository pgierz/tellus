# Security Guide

This document describes the security features and best practices for the Tellus REST API.

## API Key Authentication

### Overview

Tellus supports optional API key authentication to protect write operations (POST, PUT, DELETE). When enabled, all data-modifying operations require a valid API key.

### Authentication Behavior

- **When authentication is DISABLED** (`TELLUS_API_KEY` not set):
  - All endpoints are accessible without authentication
  - ⚠️ **NOT recommended for production deployments**
  - Suitable for local development only

- **When authentication is ENABLED** (`TELLUS_API_KEY` is set):
  - GET operations: No authentication required (read-only)
  - POST/PUT/DELETE operations: Require valid API key
  - Health endpoints: Always accessible without authentication

### Generating an API Key

Use the provided script to generate a cryptographically secure API key:

```bash
# Generate a standard API key (64 characters)
python scripts/generate-api-key.py

# Generate a longer API key (128 characters)
python scripts/generate-api-key.py --length 64

# Add to .env file
echo "TELLUS_API_KEY=$(python scripts/generate-api-key.py)" >> .env
```

### Configuring Authentication

1. **Generate a secure API key** (see above)

2. **Set the environment variable**:

   **Option 1: Using .env file** (recommended):
   ```bash
   # Add to .env
   TELLUS_API_KEY=your_secure_api_key_here
   ```

   **Option 2: Export environment variable**:
   ```bash
   export TELLUS_API_KEY=your_secure_api_key_here
   ```

   **Option 3: Docker Compose** (production):
   ```yaml
   # docker-compose.prod.yml
   services:
     tellus-api:
       environment:
         - TELLUS_API_KEY=${TELLUS_API_KEY}  # Reads from .env
   ```

3. **Restart the API server** to apply changes

### Using API Keys

Clients can provide API keys in two ways:

**Option 1: X-API-Key header** (recommended):
```bash
curl -H "X-API-Key: your_api_key_here" \
     -X POST \
     -H "Content-Type: application/json" \
     -d '{"simulation_id": "test", "model_id": "AWI-CM"}' \
     https://tellus.awi.de/api/v0/simulations/
```

**Option 2: Authorization Bearer token**:
```bash
curl -H "Authorization: Bearer your_api_key_here" \
     -X POST \
     -H "Content-Type: application/json" \
     -d '{"simulation_id": "test", "model_id": "AWI-CM"}' \
     https://tellus.awi.de/api/v0/simulations/
```

### Python SDK Usage

```python
import httpx

# Create an authenticated client
client = httpx.Client(
    base_url="https://tellus.awi.de/api/v0",
    headers={"X-API-Key": "your_api_key_here"}
)

# Make authenticated requests
response = client.post("/simulations/", json={
    "simulation_id": "CMIP6_historical",
    "model_id": "AWI-CM-1-1-MR"
})
```

### Error Responses

**Missing API Key** (when authentication is enabled):
```json
{
  "detail": "Invalid or missing API key"
}
```
HTTP Status: 401 Unauthorized

**Invalid API Key**:
```json
{
  "detail": "Invalid or missing API key"
}
```
HTTP Status: 401 Unauthorized

## Security Best Practices

### 1. API Key Management

- ✅ **DO**:
  - Generate keys using `scripts/generate-api-key.py`
  - Use keys at least 64 characters long (32 bytes)
  - Store keys in environment variables or secure secret management
  - Rotate keys periodically (every 90 days recommended)
  - Use different keys for different environments (dev, staging, prod)

- ❌ **DON'T**:
  - Commit API keys to version control
  - Share keys via email or chat
  - Use simple or predictable keys
  - Reuse keys across different systems
  - Store keys in plaintext configuration files

### 2. Network Security

**HTTPS Only**: Always use HTTPS in production:
```nginx
# nginx/conf.d/tellus-api.conf enforces HTTPS
server {
    listen 80;
    return 301 https://$server_name$request_uri;  # Redirect HTTP to HTTPS
}
```

**CORS Configuration**: Restrict allowed origins:
```bash
# .env
CORS_ORIGINS=https://tellus.awi.de,https://dashboard.awi.de
```

**Rate Limiting**: Nginx enforces rate limits:
- API endpoints: 100 requests/minute per IP
- Health endpoints: 300 requests/minute per IP

### 3. Deployment Security

**Enable Authentication**:
```bash
# ALWAYS set API key for production
TELLUS_API_KEY=$(python scripts/generate-api-key.py)
```

**Use Strong PostgreSQL Passwords**:
```bash
# .env
POSTGRES_PASSWORD=$(openssl rand -base64 32)
```

**Run Preflight Checks**:
```bash
# Before deployment
./deploy-preflight.sh
```

**Monitor API Access**:
```bash
# Check API logs
docker compose -f docker-compose.prod.yml logs -f tellus-api

# Check nginx access logs
docker compose -f docker-compose.prod.yml exec nginx tail -f /var/log/nginx/tellus-api-access.log
```

### 4. Database Security

- Use strong PostgreSQL passwords (16+ characters)
- Restrict database access to internal network only
- Enable regular automated backups
- Use encrypted connections for remote database access

### 5. Container Security

**Resource Limits** (prevents DoS):
```yaml
# docker-compose.prod.yml
services:
  tellus-api:
    deploy:
      resources:
        limits:
          cpus: '4'
          memory: 4G
        reservations:
          cpus: '1'
          memory: 1G
```

**Run as Non-Root User**:
```dockerfile
# Dockerfile already uses non-root user
USER 1000:1000
```

## Security Checklist

Before deploying to production:

- [ ] API key authentication enabled (`TELLUS_API_KEY` set)
- [ ] API key is at least 64 characters and randomly generated
- [ ] HTTPS/SSL certificates configured and valid
- [ ] CORS origins restricted to known domains
- [ ] PostgreSQL password is strong (16+ characters)
- [ ] Rate limiting enabled in nginx
- [ ] Security headers configured (CSP, HSTS, X-Frame-Options)
- [ ] Container resource limits set
- [ ] Automated backups configured
- [ ] Monitoring and logging enabled
- [ ] Preflight checks pass: `./deploy-preflight.sh`

## Reporting Security Issues

If you discover a security vulnerability, please report it to:

- Email: security@awi.de
- GitHub Security Advisories: https://github.com/pgierz/tellus/security/advisories

**Please DO NOT create public GitHub issues for security vulnerabilities.**

## Security Updates

Stay informed about security updates:

- Watch the GitHub repository for security advisories
- Subscribe to release notifications
- Review CHANGELOG.md for security-related updates

## Compliance

Tellus follows security best practices including:

- **OWASP Top 10** mitigation strategies
- **CWE/SANS Top 25** vulnerability prevention
- **NIST Cybersecurity Framework** alignment

## Additional Resources

- [OWASP API Security Top 10](https://owasp.org/www-project-api-security/)
- [FastAPI Security Documentation](https://fastapi.tiangolo.com/tutorial/security/)
- [Docker Security Best Practices](https://docs.docker.com/develop/security-best-practices/)
- [Nginx Security Controls](https://www.nginx.com/blog/mitigating-owasp-top-10-web-application-vulnerabilities-nginx/)
