# Tellus API - Quick Start Production Deployment

This is a condensed guide for experienced DevOps engineers. For complete documentation, see [DEPLOYMENT.md](DEPLOYMENT.md).

## Prerequisites Checklist

- [ ] Ubuntu 20.04+ server with 4GB+ RAM, 2+ CPU cores
- [ ] Docker 20.10+ and Docker Compose 2.0+ installed
- [ ] Domain name with DNS A record pointing to server
- [ ] Ports 80 and 443 open in firewall

## Rapid Deployment (5 Steps)

### Step 1: Clone and Setup (2 min)

```bash
git clone https://github.com/pgierz/tellus.git
cd tellus
git checkout prep-release

# Create required directories
mkdir -p nginx/ssl nginx/certbot nginx/cache backups data/tellus
chmod 700 nginx/ssl backups
```

### Step 2: Configure Environment (3 min)

```bash
# Create environment file
cp .env.example .env

# Edit with your values
nano .env
```

**Critical settings to change:**
```bash
POSTGRES_PASSWORD=YOUR_STRONG_PASSWORD_HERE  # 16+ characters
DOMAIN=api.yourdomain.com
LETSENCRYPT_EMAIL=admin@yourdomain.com
API_WORKERS=4  # CPU count * 2
TELLUS_DATA_DIR=/path/to/your/data
```

```bash
# Secure .env file
chmod 600 .env
```

### Step 3: SSL Certificates (5 min)

```bash
# Option A: Let's Encrypt (Production)
sudo apt-get install certbot
sudo certbot certonly --standalone -d api.yourdomain.com \
  --email admin@yourdomain.com --agree-tos --non-interactive
sudo cp /etc/letsencrypt/live/api.yourdomain.com/fullchain.pem nginx/ssl/
sudo cp /etc/letsencrypt/live/api.yourdomain.com/privkey.pem nginx/ssl/
chmod 600 nginx/ssl/*.pem

# Option B: Self-Signed (Testing Only)
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout nginx/ssl/privkey.pem \
  -out nginx/ssl/fullchain.pem \
  -subj "/CN=api.yourdomain.com"
chmod 600 nginx/ssl/*.pem
```

### Step 4: Process Configuration (1 min)

```bash
# Process nginx template with domain substitution
./scripts/setup-nginx-config.sh

# Run preflight checks
./deploy-preflight.sh
```

**Fix any errors before proceeding!**

### Step 5: Deploy (2 min)

```bash
# Build and start services
docker compose -f docker-compose.prod.yml up -d --build

# Watch startup
docker compose -f docker-compose.prod.yml logs -f
```

## Verification (2 min)

```bash
# Check all services healthy
docker compose -f docker-compose.prod.yml ps

# Test health endpoint (should return 200 OK)
curl http://localhost:1968/api/prep-release/health
curl https://api.yourdomain.com/api/prep-release/health

# View API documentation
open https://api.yourdomain.com/api/prep-release/docs
```

## Post-Deployment

### Set up SSL auto-renewal

```bash
sudo crontab -e
# Add this line:
0 0,12 * * * certbot renew --quiet --deploy-hook "cd /path/to/tellus && docker compose -f docker-compose.prod.yml exec nginx nginx -s reload"
```

### Configure log rotation

```bash
sudo nano /etc/logrotate.d/tellus-nginx
```

```
/path/to/tellus/nginx-logs/*.log {
    daily
    rotate 14
    compress
    delaycompress
    notifempty
    sharedscripts
    postrotate
        docker compose -f /path/to/tellus/docker-compose.prod.yml exec nginx nginx -s reload
    endscript
}
```

## Common Operations

### View Logs

```bash
# All services
docker compose -f docker-compose.prod.yml logs -f

# Specific service
docker compose -f docker-compose.prod.yml logs -f tellus-api
docker compose -f docker-compose.prod.yml logs -f postgres
docker compose -f docker-compose.prod.yml logs -f nginx
```

### Restart Services

```bash
# Restart all
docker compose -f docker-compose.prod.yml restart

# Restart specific service
docker compose -f docker-compose.prod.yml restart tellus-api

# Reload nginx (without restart)
docker compose -f docker-compose.prod.yml exec nginx nginx -s reload
```

### Update Deployment

```bash
git pull origin prep-release
docker compose -f docker-compose.prod.yml up -d --build
docker image prune -f
```

### Manual Backup

```bash
docker compose -f docker-compose.prod.yml exec postgres \
  pg_dump -U tellus tellus | gzip > backups/manual-$(date +%Y%m%d-%H%M%S).sql.gz
```

### Restore Database

```bash
# Stop API
docker compose -f docker-compose.prod.yml stop tellus-api

# Restore
gunzip -c backups/backup-file.sql.gz | \
  docker compose -f docker-compose.prod.yml exec -T postgres \
  psql -U tellus -d tellus

# Start API
docker compose -f docker-compose.prod.yml start tellus-api
```

### Access Database

```bash
docker compose -f docker-compose.prod.yml exec postgres psql -U tellus -d tellus
```

### Shell Access

```bash
# API container
docker compose -f docker-compose.prod.yml exec tellus-api bash

# Database container
docker compose -f docker-compose.prod.yml exec postgres bash
```

## Troubleshooting

### Services Won't Start

```bash
# Check logs
docker compose -f docker-compose.prod.yml logs

# Validate configuration
docker compose -f docker-compose.prod.yml config

# Check disk space
df -h

# Check permissions
ls -la nginx/ssl backups
```

### Health Check Fails

```bash
# Test API directly
docker compose -f docker-compose.prod.yml exec tellus-api \
  curl http://localhost:1968/api/prep-release/health

# Check database connection
docker compose -f docker-compose.prod.yml exec tellus-api \
  python -c "from sqlalchemy import create_engine; import os; \
  engine = create_engine(os.getenv('DATABASE_URL')); \
  with engine.connect() as conn: print('OK')"

# Check environment variables
docker compose -f docker-compose.prod.yml exec tellus-api env | grep DB
```

### 502 Bad Gateway

```bash
# Check if API is running
docker compose -f docker-compose.prod.yml ps tellus-api

# Check nginx can reach API
docker compose -f docker-compose.prod.yml exec nginx \
  wget --spider http://tellus-api:1968/api/prep-release/health

# Test nginx config
docker compose -f docker-compose.prod.yml exec nginx nginx -t

# Restart nginx
docker compose -f docker-compose.prod.yml restart nginx
```

### SSL Certificate Issues

```bash
# Check certificate validity
openssl x509 -in nginx/ssl/fullchain.pem -text -noout

# Check certificate expiration
openssl x509 -in nginx/ssl/fullchain.pem -noout -enddate

# Test SSL connection
openssl s_client -connect yourdomain.com:443 -servername yourdomain.com
```

## Important Files

| File | Purpose |
|------|---------|
| `.env` | Environment configuration (DO NOT COMMIT) |
| `docker-compose.prod.yml` | Production orchestration |
| `DEPLOYMENT.md` | Complete deployment guide |
| `docs/DEPLOYMENT_VALIDATION.md` | Security audit report |
| `docs/DEPLOYMENT_CHECKLIST.md` | Deployment checklist |
| `deploy-preflight.sh` | Pre-deployment validation |
| `nginx/conf.d/tellus-api.conf` | Nginx configuration |
| `backups/` | Database backups |

## Critical Security Notes

1. **NEVER commit `.env` file** - Contains sensitive credentials
2. **Use strong passwords** - Minimum 16 characters
3. **Restrict CORS origins** - Don't use wildcard (*) in production
4. **Keep SSL certificates secure** - 600 permissions
5. **Regular backups** - Test restoration monthly
6. **Monitor logs** - Check for suspicious activity
7. **Update regularly** - Apply security patches

## Performance Tuning

```bash
# Adjust in .env based on your server:
API_WORKERS=8              # CPU count * 2
POSTGRES_SHARED_BUFFERS=512MB  # 25% of RAM
POSTGRES_WORK_MEM=8MB      # RAM / (max_connections * 2)
```

## Monitoring

```bash
# Resource usage
docker stats

# Disk usage
docker system df
du -sh data/tellus backups/

# Container health
docker compose -f docker-compose.prod.yml ps

# Recent logs
docker compose -f docker-compose.prod.yml logs --tail=100
```

## Emergency Contacts

Update with your information:

- **Primary Admin:** [Contact Info]
- **Hosting Provider:** [Support Info]
- **DNS Provider:** [Support Info]
- **On-Call:** [Phone Number]

## Getting Help

- **Full Documentation:** [DEPLOYMENT.md](DEPLOYMENT.md)
- **Security Report:** [docs/DEPLOYMENT_VALIDATION.md](docs/DEPLOYMENT_VALIDATION.md)
- **GitHub Issues:** https://github.com/pgierz/tellus/issues
- **API Documentation:** https://yourdomain.com/api/prep-release/docs

---

**Total Setup Time:** ~15 minutes for experienced engineers

**Note:** This is a quick reference. For production deployments, always follow the complete [Deployment Checklist](docs/DEPLOYMENT_CHECKLIST.md) and review the [Validation Report](docs/DEPLOYMENT_VALIDATION.md).
