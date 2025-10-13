# Database Backups Directory

This directory stores automated PostgreSQL database backups for the Tellus API.

## Backup Strategy

Backups are automatically created by the `postgres-backup` service defined in `docker-compose.prod.yml`.

### Retention Policy

- **Daily backups:** 7 days
- **Weekly backups:** 4 weeks
- **Monthly backups:** 6 months

### Backup Schedule

- Default: Daily at midnight (UTC)
- Configurable via `BACKUP_SCHEDULE` in `.env`

## Backup File Format

Backups are compressed SQL dumps with timestamp:

```
tellus-YYYYMMDD-HHMMSS.sql.gz
```

Example:
```
tellus-20251013-030000.sql.gz
```

## Manual Backup

Create an immediate backup:

```bash
# From project root
docker compose -f docker-compose.prod.yml exec postgres pg_dump \
  -U tellus -d tellus | gzip > backups/manual-$(date +%Y%m%d-%H%M%S).sql.gz
```

## Restore from Backup

### Complete Database Restore

```bash
# 1. Stop API to prevent writes during restore
docker compose -f docker-compose.prod.yml stop tellus-api

# 2. Restore database
gunzip -c backups/tellus-20251013-030000.sql.gz | \
  docker compose -f docker-compose.prod.yml exec -T postgres \
  psql -U tellus -d tellus

# 3. Restart API
docker compose -f docker-compose.prod.yml start tellus-api
```

### Restore Specific Table

```bash
# Extract and restore only one table
gunzip -c backups/tellus-20251013-030000.sql.gz | \
  grep -A 1000 "CREATE TABLE simulations" | \
  docker compose -f docker-compose.prod.yml exec -T postgres \
  psql -U tellus -d tellus
```

## Backup Verification

Always verify backups can be restored:

```bash
# Test backup integrity
gunzip -t backups/tellus-20251013-030000.sql.gz

# Verify backup contents
gunzip -c backups/tellus-20251013-030000.sql.gz | head -n 50

# Test restore to temporary database (recommended)
docker compose -f docker-compose.prod.yml exec postgres createdb -U tellus tellus_test
gunzip -c backups/tellus-20251013-030000.sql.gz | \
  docker compose -f docker-compose.prod.yml exec -T postgres \
  psql -U tellus -d tellus_test
docker compose -f docker-compose.prod.yml exec postgres dropdb -U tellus tellus_test
```

## Backup Monitoring

Check backup service status:

```bash
# View backup service logs
docker compose -f docker-compose.prod.yml logs postgres-backup

# Check latest backup
ls -lht backups/ | head -n 5

# View backup sizes
du -h backups/*
```

## Off-Site Backup

For disaster recovery, copy backups to external storage:

```bash
# Example: Copy to S3
aws s3 sync backups/ s3://your-bucket/tellus-backups/

# Example: Copy to remote server
rsync -avz backups/ user@backup-server:/backups/tellus/

# Example: Copy to external drive
cp backups/*.sql.gz /mnt/external-drive/tellus-backups/
```

## Disaster Recovery

In case of complete data loss:

1. Restore latest backup (see above)
2. Check application logs for data loss extent
3. Verify data integrity after restore
4. Document incident and review backup strategy

## Backup Encryption (Recommended)

Encrypt backups for additional security:

```bash
# Create encrypted backup
docker compose -f docker-compose.prod.yml exec postgres pg_dump \
  -U tellus -d tellus | gzip | \
  gpg --encrypt --recipient your-email@example.com > \
  backups/encrypted-$(date +%Y%m%d-%H%M%S).sql.gz.gpg

# Decrypt and restore
gpg --decrypt backups/encrypted-20251013-030000.sql.gz.gpg | \
  gunzip | docker compose -f docker-compose.prod.yml exec -T postgres \
  psql -U tellus -d tellus
```

## Disk Space Management

Monitor backup directory size:

```bash
# Check directory size
du -sh backups/

# List old backups
find backups/ -name "*.sql.gz" -mtime +30

# Remove old backups (be careful!)
find backups/ -name "*.sql.gz" -mtime +90 -delete
```

## Troubleshooting

### Backup Service Not Running

```bash
# Check service status
docker compose -f docker-compose.prod.yml ps postgres-backup

# View logs
docker compose -f docker-compose.prod.yml logs postgres-backup

# Restart service
docker compose -f docker-compose.prod.yml restart postgres-backup
```

### Restore Fails

- Verify backup file is not corrupted: `gunzip -t backup.sql.gz`
- Check database exists: `docker compose -f docker-compose.prod.yml exec postgres psql -U tellus -l`
- Ensure sufficient disk space
- Check PostgreSQL logs for errors

## Security Notes

- Backup files contain sensitive data - protect accordingly
- Consider encrypting backups
- Restrict directory permissions: `chmod 700 backups/`
- Do not commit backups to version control
- Store off-site backups securely
- Test restore procedures regularly
