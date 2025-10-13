# SSL Certificates Directory

This directory must contain SSL/TLS certificates for production deployment.

## Required Files

- `fullchain.pem` - Full certificate chain (certificate + intermediate CA certificates)
- `privkey.pem` - Private key (must be kept secure)

## Obtaining Certificates

### Option 1: Let's Encrypt (Recommended - Free)

```bash
# Install certbot
sudo apt-get install certbot

# Obtain certificate (requires domain pointing to server)
sudo certbot certonly --standalone \
  -d yourdomain.com \
  --email admin@yourdomain.com \
  --agree-tos \
  --non-interactive

# Copy certificates to this directory
sudo cp /etc/letsencrypt/live/yourdomain.com/fullchain.pem .
sudo cp /etc/letsencrypt/live/yourdomain.com/privkey.pem .

# Set proper permissions
chmod 600 *.pem
chown $USER:$USER *.pem
```

### Option 2: Self-Signed Certificate (Testing Only)

```bash
# Generate self-signed certificate (valid for 365 days)
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout privkey.pem \
  -out fullchain.pem \
  -subj "/CN=yourdomain.com"

chmod 600 *.pem
```

### Option 3: Commercial Certificate

1. Purchase certificate from a Certificate Authority
2. Download the certificate and private key
3. Place files in this directory as `fullchain.pem` and `privkey.pem`
4. Ensure permissions: `chmod 600 *.pem`

## Certificate Renewal

Let's Encrypt certificates expire after 90 days. Set up automatic renewal:

```bash
# Add to crontab
0 0,12 * * * certbot renew --quiet --deploy-hook "cd /path/to/tellus && docker compose -f docker-compose.prod.yml exec nginx nginx -s reload"
```

## Security Notes

- Never commit certificates to version control
- Keep private key secure (600 permissions)
- Rotate certificates before expiration
- Use strong key sizes (minimum 2048-bit RSA)
- Monitor certificate expiration dates

## Verification

Test certificate after installation:

```bash
# Check certificate details
openssl x509 -in fullchain.pem -text -noout

# Test SSL configuration
openssl s_client -connect yourdomain.com:443 -servername yourdomain.com

# Check certificate expiration
openssl x509 -in fullchain.pem -noout -enddate
```
