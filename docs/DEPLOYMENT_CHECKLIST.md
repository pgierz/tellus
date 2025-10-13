# Tellus API - Production Deployment Checklist

This checklist provides a step-by-step guide for deploying Tellus API to production. Use this in conjunction with the [Deployment Validation Report](./DEPLOYMENT_VALIDATION.md) and main [Deployment Guide](../DEPLOYMENT.md).

## Pre-Deployment Phase

### 1. Infrastructure Preparation

- [ ] Server provisioned with minimum requirements:
  - [ ] Ubuntu 20.04+ (or equivalent Linux distribution)
  - [ ] 4GB+ RAM (8GB recommended)
  - [ ] 2+ CPU cores (4+ recommended)
  - [ ] 50GB+ disk space
  - [ ] Static IP address assigned

- [ ] Docker installed (version 20.10+)
  ```bash
  docker --version
  ```

- [ ] Docker Compose installed (version 2.0+)
  ```bash
  docker compose version
  ```

- [ ] Firewall configured:
  - [ ] Port 22 (SSH) - restricted to admin IPs
  - [ ] Port 80 (HTTP) - open
  - [ ] Port 443 (HTTPS) - open
  - [ ] All other ports closed

### 2. Domain and DNS Configuration

- [ ] Domain name registered
- [ ] DNS A record created pointing to server IP
- [ ] DNS propagation verified:
  ```bash
  dig +short yourdomain.com A
  ```
- [ ] Domain resolves correctly from multiple locations

### 3. Directory Structure

- [ ] Repository cloned to deployment location
- [ ] Required directories created:
  ```bash
  mkdir -p nginx/ssl nginx/certbot nginx/cache backups data/tellus
  ```
- [ ] Proper permissions set:
  ```bash
  chmod 700 nginx/ssl backups
  chmod 755 nginx/cache nginx/certbot
  ```

### 4. SSL Certificate Acquisition

Choose one option:

**Option A: Let's Encrypt (Production)**
- [ ] Certbot installed
- [ ] Certificate obtained:
  ```bash
  sudo certbot certonly --standalone -d yourdomain.com \
    --email admin@yourdomain.com --agree-tos --non-interactive
  ```
- [ ] Certificates copied to nginx/ssl/
- [ ] Certificate permissions set (600)
- [ ] Auto-renewal configured in crontab

**Option B: Self-Signed (Testing Only)**
- [ ] Self-signed certificate generated for testing
- [ ] Noted that this is NOT for production use

### 5. Environment Configuration

- [ ] `.env` file created from `.env.example`
  ```bash
  cp .env.example .env
  ```

- [ ] Database configuration:
  - [ ] `POSTGRES_PASSWORD` - Strong password set (16+ characters)
  - [ ] `POSTGRES_USER` - Customized (if desired)
  - [ ] `POSTGRES_DB` - Customized (if desired)
  - [ ] Password strength verified (uppercase, lowercase, numbers, symbols)

- [ ] Domain configuration:
  - [ ] `DOMAIN` - Set to actual domain
  - [ ] `LETSENCRYPT_EMAIL` - Valid contact email

- [ ] API configuration:
  - [ ] `API_WORKERS` - Set based on CPU count (usually CPU * 2)
  - [ ] `API_TIMEOUT` - Appropriate for workload
  - [ ] `TELLUS_API_PORT` - Default 1968 or custom

- [ ] Security configuration:
  - [ ] `TELLUS_API_KEY` - Strong API key generated (if using authentication)
  - [ ] `CORS_ORIGINS` - Restricted to specific domains

- [ ] Data directories:
  - [ ] `TELLUS_DATA_DIR` - Points to actual data location
  - [ ] `SSH_KEY_DIR` - Configured for remote storage access

- [ ] Backup configuration:
  - [ ] `BACKUP_SCHEDULE` - Appropriate schedule set
  - [ ] Retention policies configured

- [ ] `.env` file permissions:
  ```bash
  chmod 600 .env
  ```

### 6. Nginx Configuration

- [ ] Nginx template processed:
  ```bash
  ./scripts/setup-nginx-config.sh
  ```
- [ ] Domain variable substituted in nginx config
- [ ] CORS origins configured appropriately
- [ ] Rate limiting reviewed and adjusted if needed
- [ ] Security headers present in configuration

### 7. Pre-Deployment Validation

- [ ] Run preflight check script:
  ```bash
  ./deploy-preflight.sh
  ```
- [ ] All errors resolved
- [ ] Warnings reviewed and addressed
- [ ] Docker Compose configuration validated:
  ```bash
  docker compose -f docker-compose.prod.yml config
  ```

## Deployment Phase

### 8. Initial Deployment

- [ ] Build images:
  ```bash
  docker compose -f docker-compose.prod.yml build
  ```

- [ ] Start services:
  ```bash
  docker compose -f docker-compose.prod.yml up -d
  ```

- [ ] Verify all containers started:
  ```bash
  docker compose -f docker-compose.prod.yml ps
  ```

- [ ] Check for errors in logs:
  ```bash
  docker compose -f docker-compose.prod.yml logs
  ```

### 9. Service Verification

- [ ] PostgreSQL health:
  ```bash
  docker compose -f docker-compose.prod.yml exec postgres pg_isready -U tellus
  ```

- [ ] API health (internal):
  ```bash
  curl http://localhost:1968/api/prep-release/health
  ```

- [ ] Nginx health:
  ```bash
  curl http://localhost/api/prep-release/health
  ```

- [ ] HTTPS endpoint:
  ```bash
  curl https://yourdomain.com/api/prep-release/health
  ```

- [ ] API documentation accessible:
  ```bash
  curl https://yourdomain.com/api/prep-release/docs
  ```

### 10. Database Initialization

- [ ] Database schema created (automatic via entrypoint)
- [ ] Verify tables exist:
  ```bash
  docker compose -f docker-compose.prod.yml exec postgres psql -U tellus -d tellus -c "\dt"
  ```

- [ ] Test database connectivity from API:
  ```bash
  docker compose -f docker-compose.prod.yml exec tellus-api \
    python -c "from sqlalchemy import create_engine; import os; \
    engine = create_engine(os.getenv('DATABASE_URL')); \
    with engine.connect() as conn: print('DB OK')"
  ```

### 11. Backup System Verification

- [ ] Backup service running:
  ```bash
  docker compose -f docker-compose.prod.yml ps postgres-backup
  ```

- [ ] Backup directory writable
- [ ] Create manual test backup:
  ```bash
  docker compose -f docker-compose.prod.yml exec postgres \
    pg_dump -U tellus tellus | gzip > backups/test-backup.sql.gz
  ```

- [ ] Verify backup integrity:
  ```bash
  gunzip -t backups/test-backup.sql.gz
  ```

### 12. Security Verification

- [ ] HTTPS redirect working (HTTP → HTTPS)
- [ ] SSL certificate valid:
  ```bash
  openssl s_client -connect yourdomain.com:443 -servername yourdomain.com
  ```

- [ ] Security headers present:
  ```bash
  curl -I https://yourdomain.com/api/prep-release/health
  ```

- [ ] Rate limiting functional:
  ```bash
  # Test by making rapid requests
  for i in {1..150}; do curl https://yourdomain.com/api/prep-release/health; done
  ```

- [ ] CORS policy appropriate for environment
- [ ] Sensitive files blocked (test /.env, /.git)

## Post-Deployment Phase

### 13. Monitoring Setup

- [ ] Log rotation configured:
  ```bash
  # Add logrotate configuration for nginx logs
  sudo nano /etc/logrotate.d/tellus-nginx
  ```

- [ ] Resource usage baseline established:
  ```bash
  docker stats --no-stream
  ```

- [ ] Disk space monitoring configured
- [ ] Container health monitoring in place

### 14. Backup Validation

- [ ] Wait for first automated backup to complete
- [ ] Verify backup file created in backups/
- [ ] Test backup restoration:
  ```bash
  # Create test database and restore
  docker compose -f docker-compose.prod.yml exec postgres createdb -U tellus tellus_test
  gunzip -c backups/latest.sql.gz | \
    docker compose -f docker-compose.prod.yml exec -T postgres \
    psql -U tellus -d tellus_test
  # Verify and cleanup
  docker compose -f docker-compose.prod.yml exec postgres dropdb -U tellus tellus_test
  ```

- [ ] Backup retention policy working correctly
- [ ] Off-site backup strategy configured (if required)

### 15. Documentation

- [ ] Deployment details documented:
  - [ ] Server IP and credentials stored securely
  - [ ] Domain and DNS configuration documented
  - [ ] SSL certificate details noted
  - [ ] Database credentials in secure vault
  - [ ] API keys securely stored

- [ ] Runbook created for common operations:
  - [ ] Service restart procedures
  - [ ] Backup and restore procedures
  - [ ] Update/upgrade procedures
  - [ ] Troubleshooting guide

- [ ] Contact information for support documented
- [ ] Escalation procedures defined

### 16. Performance Testing

- [ ] Load testing performed:
  ```bash
  # Example with Apache Bench
  ab -n 1000 -c 10 https://yourdomain.com/api/prep-release/health
  ```

- [ ] Response times acceptable
- [ ] Resource usage under load monitored
- [ ] Rate limiting effectiveness verified
- [ ] Database query performance acceptable

### 17. Disaster Recovery Testing

- [ ] Simulate database failure and restore
- [ ] Simulate API container failure and recovery
- [ ] Simulate full system failure and recovery
- [ ] Document RTO (Recovery Time Objective) achieved
- [ ] Document RPO (Recovery Point Objective) achieved

### 18. Operational Procedures

- [ ] SSL certificate renewal process tested:
  ```bash
  sudo certbot renew --dry-run
  ```

- [ ] Update procedure documented and tested
- [ ] Rollback procedure documented
- [ ] Scaling procedure documented (if applicable)

### 19. Security Hardening

- [ ] SSH key-only authentication configured
- [ ] Root SSH login disabled
- [ ] fail2ban configured
- [ ] Automatic security updates enabled
- [ ] Docker audit logging enabled (if required)
- [ ] Container security scanning scheduled

### 20. Compliance and Governance

- [ ] Data retention policies implemented
- [ ] Audit logging configured (if required)
- [ ] Privacy policy compliance verified
- [ ] Security policies documented
- [ ] Change management procedures established

## Ongoing Operations Checklist

### Daily

- [ ] Check service status:
  ```bash
  docker compose -f docker-compose.prod.yml ps
  ```
- [ ] Review error logs for issues
- [ ] Verify latest backup created

### Weekly

- [ ] Review access logs for anomalies
- [ ] Check disk space usage
- [ ] Verify backup integrity
- [ ] Review security logs

### Monthly

- [ ] Test backup restoration
- [ ] Update Docker images:
  ```bash
  docker compose -f docker-compose.prod.yml pull
  docker compose -f docker-compose.prod.yml up -d
  ```
- [ ] Review and rotate logs
- [ ] Security scan of containers
- [ ] Review resource usage trends
- [ ] Update dependencies if needed

### Quarterly

- [ ] Full disaster recovery drill
- [ ] Security audit
- [ ] SSL certificate renewal check
- [ ] Review and update documentation
- [ ] Capacity planning review
- [ ] Performance optimization review

### Annually

- [ ] Complete security assessment
- [ ] Infrastructure review
- [ ] Update disaster recovery plan
- [ ] Review and update policies
- [ ] Contract and subscription renewals

## Emergency Contacts

Update with your specific contacts:

- **Primary Admin:** [Name, Email, Phone]
- **Secondary Admin:** [Name, Email, Phone]
- **Security Contact:** [Email]
- **Hosting Provider Support:** [Phone, Email, Portal]
- **DNS Provider Support:** [Phone, Email, Portal]
- **SSL Certificate Provider:** [Contact Info]

## Sign-Off

### Pre-Deployment Sign-Off

- [ ] Development Team Lead: _____________ Date: _______
- [ ] DevOps Engineer: _____________ Date: _______
- [ ] Security Officer: _____________ Date: _______

### Post-Deployment Sign-Off

- [ ] Deployment Successful: _____________ Date: _______
- [ ] All Tests Passed: _____________ Date: _______
- [ ] Documentation Complete: _____________ Date: _______
- [ ] Production Approval: _____________ Date: _______

---

**Notes:**

Use this checklist for every production deployment. Check off items as completed and note any deviations or issues encountered. Archive completed checklists for audit purposes.

**Related Documents:**
- [Deployment Guide](../DEPLOYMENT.md) - Detailed deployment instructions
- [Deployment Validation Report](./DEPLOYMENT_VALIDATION.md) - Security audit and validation
- [README](../README.md) - Project overview
