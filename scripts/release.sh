#!/bin/bash
# Release helper script for Tellus

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

usage() {
    echo "Usage: $0 <version>"
    echo ""
    echo "Examples:"
    echo "  $0 0.1.0a2    # Next alpha release"
    echo "  $0 0.1.0b1    # First beta release"  
    echo "  $0 0.1.0rc1   # Release candidate"
    echo "  $0 0.1.0      # Stable release"
    echo ""
    echo "This script will:"
    echo "  1. Update pyproject.toml version"
    echo "  2. Commit the version bump"
    echo "  3. Create and push a git tag"
    echo "  4. GitHub Actions will automatically build and publish to PyPI"
}

if [ $# -eq 0 ]; then
    usage
    exit 1
fi

VERSION="$1"

# Validate version format
if ! echo "$VERSION" | grep -E "^[0-9]+\.[0-9]+\.[0-9]+([ab][0-9]+|rc[0-9]+)?$" > /dev/null; then
    echo -e "${RED}Error: Invalid version format. Use semantic versioning (e.g., 0.1.0, 0.1.0a1, 0.1.0b1, 0.1.0rc1)${NC}"
    exit 1
fi

echo -e "${YELLOW}Preparing release $VERSION...${NC}"

# Check if we're on main branch
BRANCH=$(git branch --show-current)
if [ "$BRANCH" != "main" ]; then
    echo -e "${YELLOW}Warning: You're on branch '$BRANCH', not 'main'. Continue? (y/N)${NC}"
    read -r response
    if [ "$response" != "y" ] && [ "$response" != "Y" ]; then
        echo "Aborted."
        exit 1
    fi
fi

# Check for uncommitted changes
if ! git diff --quiet || ! git diff --cached --quiet; then
    echo -e "${RED}Error: You have uncommitted changes. Please commit or stash them first.${NC}"
    exit 1
fi

# Update version in pyproject.toml
echo -e "${YELLOW}Updating version in pyproject.toml...${NC}"
sed -i.bak "s/^version = .*/version = \"$VERSION\"/" pyproject.toml
rm pyproject.toml.bak

# Commit version bump
echo -e "${YELLOW}Committing version bump...${NC}"
git add pyproject.toml
git commit -m "Bump version to $VERSION"

# Create and push tag
echo -e "${YELLOW}Creating tag v$VERSION...${NC}"
git tag "v$VERSION"

echo -e "${YELLOW}Pushing changes and tag...${NC}"
git push origin "$BRANCH"
git push origin "v$VERSION"

echo -e "${GREEN}✅ Release $VERSION initiated!${NC}"
echo ""
echo "The GitHub Actions workflow will now:"
echo "  - Build the package"
echo "  - Run tests"
if echo "$VERSION" | grep -E "(a|b|rc)" > /dev/null; then
    echo "  - Publish to TestPyPI (pre-release detected)"
else
    echo "  - Publish to PyPI (stable release)"
fi
echo ""
echo "Monitor the build at: https://github.com/pgierz/tellus/actions"
echo ""
if echo "$VERSION" | grep -E "(a|b|rc)" > /dev/null; then
    echo "Once published, install with: pip install --pre tellus==$VERSION"
else
    echo "Once published, install with: pip install tellus==$VERSION"
fi
