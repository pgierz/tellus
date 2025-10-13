# Tellus REST API Production Deployment Guide

## Overview

This guide covers deploying the Tellus REST API to a production server using Docker Compose with PostgreSQL, Nginx reverse proxy, SSL certificates, and automated backups.

## Architecture

```
Internet → Nginx (SSL) → Tellus API (FastAPI/Uvicorn) → PostgreSQL
                              ↓
                         File System (Simulations/Locations)
                              ↓
                    Remote Storage (SSH/SFTP/Cloud)
```

## Prerequisites

### Server Requirements

- **OS**: Linux (Ubuntu 20.04+ or similar)
- **RAM**: Minimum 4GB, recommended 8GB+
- **CPU**: Minimum 2 cores, recommended 4+ cores
- **Disk**: Minimum 50GB for database and cache
- **Docker**: Version 20.10+
- **Docker Compose**: Version 2.0+

### Domain Setup

- Domain name pointing to your server's IP address
- DNS A record configured (e.g., `api.your-domain.com`)
- Firewall rules allowing ports 80 (HTTP) and 443 (HTTPS)

## Step 1: Initial Server Setup

### 1.1 Install Docker and Docker Compose

```bash
# Update system
sudo apt-get update && sudo apt-get upgrade -y

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Add your user to docker group
sudo usermod -aG docker $USER

# Install Docker Compose (v2)
sudo apt-get install docker-compose-plugin

# Verify installation
docker --version
docker compose version
```

### 1.2 Configure Firewall

```bash
# Allow SSH
sudo ufw allow 22/tcp

# Allow HTTP and HTTPS
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp

# Enable firewall
sudo ufw enable
```

### 1.3 Create Application Directory

```bash
# Create directory structure
mkdir -p ~/tellus-api
cd ~/tellus-api

# Create required directories
mkdir -p data/tellus backups nginx/ssl logs
```

## Step 2: Deploy Tellus API

### 2.1 Clone Repository

```bash
cd ~/tellus-api
git clone https://github.com/pgierz/tellus.git .
git checkout prep-release
```

### 2.2 Configure Environment

```bash
# Copy environment template
cp .env.example .env

# Edit with your settings
nano .env
```

**Critical settings to configure in `.env`:**

```bash
# Database password (REQUIRED - use a strong password!)
POSTGRES_PASSWORD=your_secure_password_here

# Your domain name
DOMAIN=api.your-domain.com

# Email for Let's Encrypt
LETSENCRYPT_EMAIL=admin@your-domain.com

# API workers (usually CPU count * 2)
API_WORKERS=4

# Data directory (where simulations.json lives)
TELLUS_DATA_DIR=/path/to/your/tellus/data

# SSH keys (for remote storage access)
SSH_KEY_DIR=/home/username/.ssh
```

### 2.3 Obtain SSL Certificates

**Option A: Let's Encrypt (Recommended)**

```bash
# Install Certbot
sudo apt-get install certbot

# Stop nginx if running
docker compose -f docker-compose.prod.yml stop nginx

# Get certificate
sudo certbot certonly --standalone \
  -d api.your-domain.com \
  --email admin@your-domain.com \
  --agree-tos \
  --non-interactive

# Copy certificates to nginx directory
sudo cp /etc/letsencrypt/live/api.your-domain.com/fullchain.pem nginx/ssl/
sudo cp /etc/letsencrypt/live/api.your-domain.com/privkey.pem nginx/ssl/

# Set permissions
sudo chown $USER:$USER nginx/ssl/*.pem
chmod 600 nginx/ssl/*.pem
```

**Option B: Self-Signed Certificate (Testing Only)**

```bash
# Generate self-signed certificate
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout nginx/ssl/privkey.pem \
  -out nginx/ssl/fullchain.pem \
  -subj "/CN=api.your-domain.com"

chmod 600 nginx/ssl/*.pem
```

### 2.4 Update Nginx Configuration

```bash
# Replace ${DOMAIN} placeholder in nginx config
sed -i "s/\${DOMAIN}/$DOMAIN/g" nginx/conf.d/tellus-api.conf
```

### 2.5 Start Services

```bash
# Pull latest images (if using pre-built)
docker compose -f docker-compose.prod.yml pull

# Build and start services
docker compose -f docker-compose.prod.yml up -d --build

# Check logs
docker compose -f docker-compose.prod.yml logs -f
```

### 2.6 Verify Deployment

```bash
# Check container status
docker compose -f docker-compose.prod.yml ps

# Test health endpoint
curl http://localhost:1968/api/prep-release/health

# Test through Nginx (HTTP)
curl http://api.your-domain.com/api/prep-release/health

# Test through Nginx (HTTPS)
curl https://api.your-domain.com/api/prep-release/health

# View API documentation
open https://api.your-domain.com/docs
```

## Step 3: Initialize Database

### 3.1 Database Schema

The database schema is automatically created on first startup by the `docker-entrypoint.sh` script. To manually trigger it:

```bash
docker compose -f docker-compose.prod.yml exec tellus-api python -c "
from sqlalchemy import create_engine
from tellus.infrastructure.database.models import Base
import os

database_url = os.getenv('DATABASE_URL')
engine = create_engine(database_url)
Base.metadata.create_all(engine)
print('✓ Database schema created')
"
```

### 3.2 Verify Database

```bash
# Connect to PostgreSQL
docker compose -f docker-compose.prod.yml exec postgres psql -U tellus -d tellus

# List tables
\dt

# Check schema
\d simulations
\d locations
\d workflows

# Exit
\q
```

## Step 4: Configure Automated Backups

### 4.1 Backup Service

The `postgres-backup` service in `docker-compose.prod.yml` automatically backs up PostgreSQL daily. Backups are stored in `./backups/`.

**Backup schedule** (configured via `.env`):
```bash
BACKUP_SCHEDULE=@daily  # Daily at midnight
BACKUP_KEEP_DAYS=7      # Keep 7 daily backups
BACKUP_KEEP_WEEKS=4     # Keep 4 weekly backups
BACKUP_KEEP_MONTHS=6    # Keep 6 monthly backups
```

### 4.2 Manual Backup

```bash
# Create manual backup
docker compose -f docker-compose.prod.yml exec postgres pg_dump \
  -U tellus -d tellus | gzip > backups/manual-backup-$(date +%Y%m%d-%H%M%S).sql.gz

# List backups
ls -lh backups/
```

### 4.3 Restore from Backup

```bash
# Stop API to prevent writes
docker compose -f docker-compose.prod.yml stop tellus-api

# Restore database
gunzip -c backups/tellus-YYYYMMDD-HHMMSS.sql.gz | \
  docker compose -f docker-compose.prod.yml exec -T postgres \
  psql -U tellus -d tellus

# Start API
docker compose -f docker-compose.prod.yml start tellus-api
```

## Step 5: Monitoring and Maintenance

### 5.1 View Logs

```bash
# All services
docker compose -f docker-compose.prod.yml logs -f

# Specific service
docker compose -f docker-compose.prod.yml logs -f tellus-api
docker compose -f docker-compose.prod.yml logs -f postgres
docker compose -f docker-compose.prod.yml logs -f nginx

# Nginx access logs
tail -f logs/access.log

# Nginx error logs
tail -f logs/error.log
```

### 5.2 Resource Usage

```bash
# Container stats
docker stats

# Disk usage
docker system df

# Volume usage
du -sh data/tellus
du -sh backups/
```

### 5.3 Update Deployment

```bash
# Pull latest code
cd ~/tellus-api
git pull origin prep-release

# Rebuild and restart
docker compose -f docker-compose.prod.yml up -d --build

# Clean up old images
docker image prune -f
```

### 5.4 Restart Services

```bash
# Restart all services
docker compose -f docker-compose.prod.yml restart

# Restart specific service
docker compose -f docker-compose.prod.yml restart tellus-api

# Graceful reload nginx
docker compose -f docker-compose.prod.yml exec nginx nginx -s reload
```

## Step 6: SSL Certificate Renewal

### 6.1 Auto-renewal with Certbot

```bash
# Set up cron job for auto-renewal
sudo crontab -e

# Add this line (runs twice daily)
0 0,12 * * * certbot renew --quiet --deploy-hook "cd ~/tellus-api && docker compose -f docker-compose.prod.yml exec nginx nginx -s reload"
```

### 6.2 Manual Renewal

```bash
# Renew certificate
sudo certbot renew

# Copy new certificates
sudo cp /etc/letsencrypt/live/api.your-domain.com/fullchain.pem ~/tellus-api/nginx/ssl/
sudo cp /etc/letsencrypt/live/api.your-domain.com/privkey.pem ~/tellus-api/nginx/ssl/

# Reload nginx
docker compose -f docker-compose.prod.yml exec nginx nginx -s reload
```

## Troubleshooting

### Issue: Health Check Failing

```bash
# Check if API is running
docker compose -f docker-compose.prod.yml ps tellus-api

# Check API logs
docker compose -f docker-compose.prod.yml logs tellus-api

# Test API directly (bypass nginx)
docker compose -f docker-compose.prod.yml exec tellus-api curl http://localhost:1968/api/prep-release/health

# Check database connection
docker compose -f docker-compose.prod.yml exec tellus-api python -c "
from sqlalchemy import create_engine
import os
engine = create_engine(os.getenv('DATABASE_URL'))
with engine.connect() as conn:
    print('✓ Database connection successful')
"
```

### Issue: Database Connection Failed

```bash
# Check PostgreSQL is running
docker compose -f docker-compose.prod.yml ps postgres

# Check PostgreSQL logs
docker compose -f docker-compose.prod.yml logs postgres

# Test connection
docker compose -f docker-compose.prod.yml exec postgres pg_isready -U tellus

# Check environment variables
docker compose -f docker-compose.prod.yml exec tellus-api env | grep DB
```

### Issue: Nginx Returns 502 Bad Gateway

```bash
# Check if API is accessible from nginx
docker compose -f docker-compose.prod.yml exec nginx curl http://tellus-api:1968/api/prep-release/health

# Check nginx error logs
docker compose -f docker-compose.prod.yml logs nginx

# Test nginx configuration
docker compose -f docker-compose.prod.yml exec nginx nginx -t

# Reload nginx config
docker compose -f docker-compose.prod.yml exec nginx nginx -s reload
```

### Issue: Out of Disk Space

```bash
# Check disk usage
df -h

# Clean Docker
docker system prune -a --volumes -f

# Remove old backups
find backups/ -name "*.sql.gz" -mtime +30 -delete

# Clear PostgreSQL WAL files
docker compose -f docker-compose.prod.yml exec postgres pg_ctl checkpoint
```

## Security Checklist

- [ ] Strong PostgreSQL password set in `.env`
- [ ] `.env` file permissions set to 600 (`chmod 600 .env`)
- [ ] SSL certificates installed and valid
- [ ] Firewall configured (only 80, 443, 22 open)
- [ ] SSH key authentication enabled (disable password auth)
- [ ] Rate limiting configured in Nginx
- [ ] Database backups running daily
- [ ] Monitoring set up (Prometheus/Grafana - optional)
- [ ] Log rotation configured
- [ ] Docker socket not exposed to containers
- [ ] Non-root user in containers (already configured)
- [ ] Resource limits set (`deploy.resources` in compose file)

## Performance Tuning

### API Workers

Adjust based on CPU cores:
```bash
# In .env
API_WORKERS=8  # Usually CPU count * 2
```

### PostgreSQL Tuning

Edit `docker-compose.prod.yml`:
```yaml
environment:
  POSTGRES_SHARED_BUFFERS: 512MB    # 25% of RAM
  POSTGRES_WORK_MEM: 8MB            # RAM / (max_connections * 2)
  POSTGRES_MAINTENANCE_WORK_MEM: 128MB
  POSTGRES_EFFECTIVE_CACHE_SIZE: 2GB  # 50-75% of RAM
```

### Nginx Caching

Add to `nginx/conf.d/tellus-api.conf`:
```nginx
# Cache zone for API responses
proxy_cache_path /var/cache/nginx levels=1:2 keys_zone=api_cache:10m max_size=100m inactive=60m;

# Use cache for specific endpoints
location /api/prep-release/simulations {
    proxy_cache api_cache;
    proxy_cache_valid 200 5m;
    proxy_cache_key "$request_uri";
    # ... rest of proxy config
}
```

## Production Checklist

Before going live:

- [ ] `.env` configured with production values
- [ ] SSL certificates installed
- [ ] Domain DNS configured
- [ ] Health check endpoint returns 200 OK
- [ ] API documentation accessible at `/docs`
- [ ] Database backup service running
- [ ] Test backup restore procedure
- [ ] Logs being written to expected locations
- [ ] Resource usage within acceptable limits
- [ ] All services restart after reboot (`restart: unless-stopped`)
- [ ] SSH keys mounted for remote storage access
- [ ] Simulation/location data directory mounted
- [ ] Rate limiting tested
- [ ] CORS configured for web UI origin

## Advanced: Multi-Server Deployment

For high availability:

1. **Separate Database Server**: Run PostgreSQL on dedicated server
2. **Load Balancer**: Use HAProxy or cloud load balancer
3. **Shared Storage**: NFS or cloud object storage for simulation data
4. **Redis Cache**: Add Redis for session/query caching
5. **Monitoring Stack**: Prometheus + Grafana + Loki

See `docs/architecture/high-availability.md` for detailed guide (TBD).

## Support

For issues:
- Check logs: `docker compose -f docker-compose.prod.yml logs`
- GitHub Issues: https://github.com/pgierz/tellus/issues
- Health check: `curl https://api.your-domain.com/api/prep-release/health`
- API docs: `https://api.your-domain.com/docs`

## Quick Reference

### Common Commands

```bash
# Start
docker compose -f docker-compose.prod.yml up -d

# Stop
docker compose -f docker-compose.prod.yml down

# Restart
docker compose -f docker-compose.prod.yml restart

# Update
git pull && docker compose -f docker-compose.prod.yml up -d --build

# Logs
docker compose -f docker-compose.prod.yml logs -f

# Backup
docker compose -f docker-compose.prod.yml exec postgres pg_dump -U tellus tellus | gzip > backup.sql.gz

# Shell access
docker compose -f docker-compose.prod.yml exec tellus-api bash
docker compose -f docker-compose.prod.yml exec postgres psql -U tellus -d tellus
```

### Useful Endpoints

- Health: `https://api.your-domain.com/api/prep-release/health`
- API Docs: `https://api.your-domain.com/docs`
- OpenAPI Schema: `https://api.your-domain.com/openapi.json`
- Simulations: `https://api.your-domain.com/api/prep-release/simulations`
- Locations: `https://api.your-domain.com/api/prep-release/locations`
