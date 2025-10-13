#!/bin/bash
# Tellus API - Production Deployment Preflight Check
# Run this script before deploying to production to validate configuration

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

errors=0
warnings=0

echo "========================================="
echo "Tellus API - Deployment Preflight Check"
echo "========================================="
echo ""

# Function to print status messages
print_error() {
    echo -e "${RED}ERROR:${NC} $1"
    errors=$((errors + 1))
}

print_warning() {
    echo -e "${YELLOW}WARNING:${NC} $1"
    warnings=$((warnings + 1))
}

print_success() {
    echo -e "${GREEN}OK:${NC} $1"
}

# Check 1: .env file exists
echo "[1/15] Checking for .env file..."
if [ ! -f .env ]; then
    print_error ".env file not found. Copy .env.example to .env and configure"
else
    print_success ".env file exists"
fi

# Check 2: Default passwords
echo "[2/15] Checking for default passwords..."
if [ -f .env ]; then
    if grep -q "CHANGE_ME" .env; then
        print_error "Default password 'CHANGE_ME' found in .env. Set strong passwords!"
    else
        print_success "No default passwords detected"
    fi
fi

# Check 3: Database password strength
echo "[3/15] Checking database password strength..."
if [ -f .env ]; then
    POSTGRES_PASSWORD=$(grep "^POSTGRES_PASSWORD=" .env | cut -d'=' -f2)
    if [ -n "$POSTGRES_PASSWORD" ]; then
        if [ ${#POSTGRES_PASSWORD} -lt 16 ]; then
            print_error "Database password is too short (minimum 16 characters)"
        elif [[ ! "$POSTGRES_PASSWORD" =~ [A-Z] ]] || [[ ! "$POSTGRES_PASSWORD" =~ [a-z] ]] || [[ ! "$POSTGRES_PASSWORD" =~ [0-9] ]]; then
            print_warning "Database password should contain uppercase, lowercase, and numbers"
        else
            print_success "Database password meets strength requirements"
        fi
    else
        print_error "POSTGRES_PASSWORD not set in .env"
    fi
fi

# Check 4: Required directories
echo "[4/15] Checking required directories..."
required_dirs=("nginx/ssl" "nginx/certbot" "nginx/cache" "backups" "data/tellus")
for dir in "${required_dirs[@]}"; do
    if [ ! -d "$dir" ]; then
        print_error "Required directory missing: $dir"
    else
        print_success "Directory exists: $dir"
    fi
done

# Check 5: SSL certificates
echo "[5/15] Checking SSL certificates..."
if [ ! -f nginx/ssl/fullchain.pem ] || [ ! -f nginx/ssl/privkey.pem ]; then
    print_error "SSL certificates not found in nginx/ssl/ (fullchain.pem, privkey.pem)"
else
    # Check certificate expiration
    expiry_date=$(openssl x509 -in nginx/ssl/fullchain.pem -noout -enddate 2>/dev/null | cut -d= -f2)
    if [ -n "$expiry_date" ]; then
        expiry_timestamp=$(date -d "$expiry_date" +%s 2>/dev/null || date -j -f "%b %d %H:%M:%S %Y %Z" "$expiry_date" +%s 2>/dev/null)
        current_timestamp=$(date +%s)
        days_until_expiry=$(( ($expiry_timestamp - $current_timestamp) / 86400 ))

        if [ $days_until_expiry -lt 0 ]; then
            print_error "SSL certificate has EXPIRED"
        elif [ $days_until_expiry -lt 30 ]; then
            print_warning "SSL certificate expires in $days_until_expiry days"
        else
            print_success "SSL certificate valid ($days_until_expiry days remaining)"
        fi
    fi

    # Check certificate permissions
    if [ -r nginx/ssl/privkey.pem ]; then
        perms=$(stat -c "%a" nginx/ssl/privkey.pem 2>/dev/null || stat -f "%OLp" nginx/ssl/privkey.pem 2>/dev/null)
        if [ "$perms" != "600" ] && [ "$perms" != "400" ]; then
            print_warning "Private key permissions should be 600 or 400 (currently: $perms)"
        else
            print_success "SSL certificate permissions correct"
        fi
    fi
fi

# Check 6: Docker and Docker Compose
echo "[6/15] Checking Docker installation..."
if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed"
else
    docker_version=$(docker --version | awk '{print $3}' | sed 's/,//')
    print_success "Docker installed: $docker_version"
fi

if ! command -v docker compose version &> /dev/null 2>&1; then
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed"
    else
        compose_version=$(docker-compose --version | awk '{print $4}' | sed 's/,//')
        print_success "Docker Compose installed: $compose_version (legacy)"
    fi
else
    compose_version=$(docker compose version --short)
    print_success "Docker Compose installed: $compose_version"
fi

# Check 7: Docker Compose file syntax
echo "[7/15] Validating docker-compose.prod.yml syntax..."
if docker compose -f docker-compose.prod.yml config > /dev/null 2>&1 || docker-compose -f docker-compose.prod.yml config > /dev/null 2>&1; then
    print_success "docker-compose.prod.yml is valid"
else
    print_error "docker-compose.prod.yml has syntax errors"
fi

# Check 8: Port availability
echo "[8/15] Checking port availability..."
check_port() {
    port=$1
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1 || netstat -tuln 2>/dev/null | grep -q ":$port "; then
        print_warning "Port $port is already in use"
        return 1
    else
        print_success "Port $port is available"
        return 0
    fi
}

check_port 80
check_port 443
if [ -f .env ]; then
    api_port=$(grep "^TELLUS_API_PORT=" .env | cut -d'=' -f2)
    if [ -n "$api_port" ]; then
        check_port $api_port
    fi
fi

# Check 9: Disk space
echo "[9/15] Checking disk space..."
available_space=$(df -h . | awk 'NR==2 {print $4}')
available_space_bytes=$(df -k . | awk 'NR==2 {print $4}')
if [ $available_space_bytes -lt 10485760 ]; then  # Less than 10GB
    print_warning "Low disk space: $available_space available (recommend 10GB+)"
else
    print_success "Sufficient disk space: $available_space available"
fi

# Check 10: Environment variables
echo "[10/15] Checking critical environment variables..."
if [ -f .env ]; then
    required_vars=("POSTGRES_PASSWORD" "DOMAIN")
    for var in "${required_vars[@]}"; do
        if ! grep -q "^$var=" .env; then
            print_error "Required variable $var not set in .env"
        else
            value=$(grep "^$var=" .env | cut -d'=' -f2)
            if [ -z "$value" ]; then
                print_error "Required variable $var is empty in .env"
            else
                print_success "Variable $var is set"
            fi
        fi
    done
fi

# Check 11: Domain configuration
echo "[11/15] Checking domain configuration..."
if [ -f .env ]; then
    domain=$(grep "^DOMAIN=" .env | cut -d'=' -f2)
    if [ "$domain" = "api.your-domain.com" ] || [ -z "$domain" ]; then
        print_warning "Domain not configured (still using default)"
    else
        print_success "Domain configured: $domain"

        # Check if domain resolves
        if command -v dig &> /dev/null; then
            if dig +short "$domain" A | grep -q .; then
                ip=$(dig +short "$domain" A | head -n1)
                print_success "Domain resolves to: $ip"
            else
                print_warning "Domain does not resolve. Configure DNS before deployment"
            fi
        fi
    fi
fi

# Check 12: Nginx configuration variables
echo "[12/15] Checking nginx configuration..."
if grep -q '${DOMAIN}' nginx/conf.d/tellus-api.conf 2>/dev/null; then
    print_warning "Nginx config contains \${DOMAIN} variable that won't be substituted"
fi

# Check 13: CORS configuration
echo "[13/15] Checking CORS configuration..."
if grep -q "set \$cors_origin \"\*\"" nginx/conf.d/tellus-api.conf 2>/dev/null; then
    print_warning "CORS allows all origins (*) - should restrict in production"
fi

# Check 14: File permissions
echo "[14/15] Checking file permissions..."
if [ -f docker-compose.prod.yml ]; then
    if [ ! -r docker-compose.prod.yml ]; then
        print_error "Cannot read docker-compose.prod.yml"
    fi
fi

if [ -f .env ]; then
    perms=$(stat -c "%a" .env 2>/dev/null || stat -f "%OLp" .env 2>/dev/null)
    if [ "$perms" != "600" ] && [ "$perms" != "400" ]; then
        print_warning ".env file should have 600 permissions (currently: $perms)"
        print_warning "Run: chmod 600 .env"
    else
        print_success ".env file permissions correct"
    fi
fi

# Check 15: Git repository check
echo "[15/15] Checking git repository status..."
if [ -d .git ]; then
    if git status --porcelain | grep -q "^??.*\.env$"; then
        print_success ".env file is not tracked in git"
    fi

    # Check for uncommitted changes
    if [ -n "$(git status --porcelain)" ]; then
        print_warning "You have uncommitted changes. Consider committing before deployment"
    else
        print_success "Git working directory is clean"
    fi
fi

echo ""
echo "========================================="
echo "Preflight Check Summary"
echo "========================================="

if [ $errors -eq 0 ] && [ $warnings -eq 0 ]; then
    echo -e "${GREEN}All checks passed!${NC} Ready for deployment."
    echo ""
    echo "Next steps:"
    echo "  1. Review configuration files one more time"
    echo "  2. Start services: docker compose -f docker-compose.prod.yml up -d"
    echo "  3. Monitor logs: docker compose -f docker-compose.prod.yml logs -f"
    echo "  4. Test endpoints after startup"
    exit 0
elif [ $errors -eq 0 ]; then
    echo -e "${YELLOW}$warnings warning(s) detected.${NC} Review warnings above."
    echo "You can proceed with deployment, but address warnings when possible."
    echo ""
    echo "To deploy anyway: docker compose -f docker-compose.prod.yml up -d"
    exit 0
else
    echo -e "${RED}$errors error(s) and $warnings warning(s) detected.${NC}"
    echo "Fix errors before deploying to production."
    echo ""
    echo "Common fixes:"
    echo "  - Create .env: cp .env.example .env"
    echo "  - Set strong password: edit POSTGRES_PASSWORD in .env"
    echo "  - Create directories: mkdir -p nginx/ssl nginx/certbot nginx/cache backups"
    echo "  - Get SSL certificates: see nginx/ssl/README.md"
    exit 1
fi
