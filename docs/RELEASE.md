# PyPI Release Setup

This document explains how to set up automated PyPI releases for Tellus.

## 🔧 One-time Setup Required

### 1. Configure PyPI Trusted Publishing

**For PyPI (stable releases):**
1. Go to https://pypi.org/manage/account/publishing/
2. Add a new trusted publisher:
   - **PyPI project name**: `tellus`
   - **Owner**: `pgierz` 
   - **Repository name**: `tellus`
   - **Workflow filename**: `publish.yml`
   - **Environment name**: `pypi`

**For TestPyPI (pre-releases):**
1. Go to https://test.pypi.org/manage/account/publishing/
2. Add the same configuration but with environment name: `testpypi`

### 2. Create GitHub Environments

1. Go to your GitHub repo → Settings → Environments
2. Create environment: `pypi`
   - Add protection rule: "Required reviewers" (yourself)
3. Create environment: `testpypi` 
   - Add protection rule: "Required reviewers" (yourself)

## 🚀 How to Release

### Option 1: Use the Release Script (Recommended)

```bash
# For pre-releases (automatically goes to TestPyPI)
./scripts/release.sh 0.1.0a2

# For stable releases (goes to main PyPI)  
./scripts/release.sh 0.1.0
```

### Option 2: Manual Tag Creation

```bash
# Update version in pyproject.toml first
git add pyproject.toml
git commit -m "Bump version to 0.1.0a2"
git tag v0.1.0a2
git push origin main --tags
```

## 📦 What Happens Automatically

1. **On tag push** → GitHub Actions triggers
2. **Build phase** → Creates wheel and source distribution
3. **Test phase** → Runs test suite (if configured)
4. **Publish phase** → 
   - Pre-releases (a, b, rc) → TestPyPI
   - Stable releases → Main PyPI
5. **Notification** → Check Actions tab for status

## 🔍 Monitoring Releases

- **GitHub Actions**: https://github.com/pgierz/tellus/actions
- **PyPI Package**: https://pypi.org/project/tellus/
- **TestPyPI Package**: https://test.pypi.org/project/tellus/

## 📋 Version Format Guidelines

- **Alpha**: `0.1.0a1`, `0.1.0a2`, etc.
- **Beta**: `0.1.0b1`, `0.1.0b2`, etc.
- **Release Candidate**: `0.1.0rc1`, `0.1.0rc2`, etc.
- **Stable**: `0.1.0`, `0.2.0`, `1.0.0`, etc.

## 🛠️ Installation for Users

```bash
# Stable releases
pip install tellus

# Latest pre-release  
pip install --pre tellus

# Specific version
pip install tellus==0.1.0a1

# From TestPyPI (pre-releases)
pip install -i https://test.pypi.org/simple/ tellus
```

## 🔮 Future: Docker Images

When ready to add Docker support, we can extend the workflow to also build and push Docker images to GitHub Container Registry on the same tag triggers.
