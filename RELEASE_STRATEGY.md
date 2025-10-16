# Tellus v0.1.0 Release Strategy

**Date**: 2025-01-13
**Current Branch**: `prep-release`
**Status**: Documentation Complete - Deciding on Feature Scope

---

## Branch Analysis Summary

### Active Branches & Worktrees

| Branch/Worktree | Status | Lines Changed | Merge Recommendation |
|-----------------|--------|---------------|----------------------|
| **prep-release** (current) | ✅ Ready | +7,460 docs | **RELEASE BASE** |
| **origin/feat/network-aware-transfers** | 🔄 Experimental | +5,446 (+5 commits ahead) | **DEFER to v0.2.0** |
| **web-ui** (worktree) | ✅ Complete | Reflex-based UI | **DEFER to v0.2.0** |
| **master** | ✅ Stable | Baseline | Merge target after release |

---

## Feature Completeness Assessment

### prep-release Branch (CURRENT - Ready for v0.1.0)

#### ✅ Complete Features
- **REST API**: Full FastAPI implementation with 40+ endpoints
- **Python SDK**: Clean aliases (`Simulation`, `Location`, `SimulationService`, `LocationService`)
- **Production Deployment**: Docker Compose + Nginx + SSL + Backups
- **Database**: PostgreSQL + SQLite support with async SQLAlchemy
- **CLI**: Rich-based terminal UI with 33+ commands
- **Documentation**:
  - `docs/SDK.md` - Complete Python SDK reference
  - `docs/API_REFERENCE.md` - REST API documentation
  - `docs/INSTALLATION.md` - Installation guide
  - `CHANGELOG.md` - Full v0.1.0 changelog
  - `DEPLOYMENT.md` - Production deployment guide
  - Deployment validation reports
- **Testing**: 22/22 tests passing
- **CI/CD**: GitHub Actions with executable documentation

#### ⚠️ Known Issues (6 CRITICAL)
Per `docs/DEPLOYMENT_VALIDATION.md`:
1. CORS wildcard (security vulnerability)
2. Missing SSL directory
3. Missing backups directory
4. Nginx domain variable substitution
5. No API authentication
6. Weak password validation

**Security Score**: 48% (29/60 points)
**Production Readiness**: CONDITIONAL - Requires critical fixes

---

### feat/network-aware-transfers Branch (EXPERIMENTAL)

#### Features (+5,446 lines, 5 commits ahead of merge base)

**Network Topology System**:
- `network_connection.py` - Network connection entities
- `network_metrics.py` - Performance metrics tracking
- `network_topology.py` - Topology management
- Network benchmarking adapter
- Comprehensive test suite (4,000+ lines of tests)
- Network-aware routing for archive staging
- CLI commands for network management

**Status**:
- ✅ Core features stable (per RELEASE_STATUS.md)
- 🔄 Partially working / In development
- ❌ CLI commands incomplete
- ❌ Configuration persistence not fully working

**Test Coverage**: Extensive unit tests, but limited integration testing

**Files Added/Modified**:
- 20 files changed
- Most changes in:
  - `src/tellus/domain/entities/` - 3 new network entities
  - `tests/unit/domain/entities/` - 3 test files (2,800+ lines)
  - `tests/unit/application/services/test_network_topology_service.py` (1,047 lines)
  - `tests/unit/infrastructure/adapters/test_network_benchmarking_adapter.py` (930 lines)
  - `src/tellus/interfaces/cli/simulation.py` - Extended CLI commands

---

### web-ui Worktree (Reflex-based Web Interface)

#### Features (COMPLETE)

**Web UI Refactoring** (Per `WEB_UI_REFACTOR_COMPLETE.md`):
- ✅ Component-first simulation structure
- ✅ Type definitions with TypedDict
- ✅ Service layer updates
- ✅ State management with Reflex
- ✅ Create simulation modal with full metadata
- ✅ Simulation list display with badges
- ✅ Simulation detail page with component breakdown
- ✅ Backward compatibility maintained

**Technology**:
- Python Reflex framework
- TailwindCSS styling
- Connects to REST API at `/api/v0a3`
- Async loading with httpx

**Status**:
- ✅ Code complete and syntax validated
- ✅ Backward compatible with legacy simulations
- ⏳ Manual integration testing pending
- ⏳ Not yet connected to prep-release branch

**Files**:
- 6 files modified
- 3 new atomic design components
- Complete type safety with TypedDict

---

## Release Decision Matrix

### Option 1: Minimal v0.1.0 (RECOMMENDED)

**Scope**: prep-release branch ONLY

**Includes**:
- ✅ REST API (core feature)
- ✅ Python SDK with aliases
- ✅ Production deployment infrastructure
- ✅ Complete documentation
- ✅ CLI (existing stable commands)
- ✅ PostgreSQL/SQLite support

**Excludes**:
- ❌ Network topology system (defer to v0.2.0)
- ❌ Web UI (defer to v0.2.0 or separate release)

**Timeline**: 1-2 weeks
- Week 1: Fix 6 critical security issues
- Week 2: Test deployment, tag and release

**Benefits**:
- Focused scope reduces risk
- Clear feature boundary
- Shorter time to market
- Network features get more testing time
- Web UI can mature independently

**Risks**:
- Low - All features are tested and documented
- Security issues are known and fixable

**Release Grade**: B+ (Production ready after critical fixes)

---

### Option 2: Enhanced v0.1.0 (NOT RECOMMENDED)

**Scope**: prep-release + feat/network-aware-transfers

**Includes**:
- Everything from Option 1
- ✅ Network topology system
- ✅ Network-aware transfer routing
- ✅ Network benchmarking

**Excludes**:
- ❌ Web UI (still defer)

**Timeline**: 3-4 weeks
- Week 1: Fix 6 critical security issues
- Week 2: Merge network branch, resolve conflicts
- Week 3: Integration testing of network features
- Week 4: Documentation updates, release

**Benefits**:
- More features in initial release
- Network-aware transfers available immediately

**Risks**:
- **HIGH** - Network features are experimental
- CLI commands incomplete (per RELEASE_STATUS.md)
- Configuration persistence not working
- Merge conflicts likely (+5,446 lines vs +7,460 docs)
- Extended testing required
- Delayed release by 2-3 weeks

**Release Grade**: C+ (Too much scope creep, risk of delays)

---

### Option 3: Full Stack v0.1.0 (NOT RECOMMENDED)

**Scope**: prep-release + network + web-ui

**Includes**:
- Everything from Option 2
- ✅ Reflex-based Web UI
- ✅ Browser-based simulation management

**Timeline**: 5-6 weeks
- Week 1: Fix critical security issues
- Week 2-3: Merge network branch, test
- Week 3-4: Integrate web-ui, test API integration
- Week 5: End-to-end testing
- Week 6: Documentation, release

**Benefits**:
- Complete feature set
- Web UI provides better UX

**Risks**:
- **VERY HIGH** - Too many moving parts
- Web UI needs manual integration testing
- Network features are experimental
- Large merge surface area
- High probability of integration issues
- Release delayed by 4-5 weeks minimum

**Release Grade**: D (Feature creep, high risk)

---

## Recommended Release Strategy: **Option 1 - Minimal v0.1.0**

### Rationale

1. **Focus on Core Value**: REST API + Python SDK is the primary value proposition
2. **Production Ready**: After critical fixes, deployment stack is solid
3. **Time to Market**: 1-2 weeks vs 3-6 weeks for alternatives
4. **Risk Reduction**: All features are tested and documented
5. **Clear Upgrade Path**: Network and Web UI become v0.2.0 features

### Roadmap

**v0.1.0 (Minimal - 1-2 weeks)**:
- REST API with OpenAPI docs
- Python SDK with convenient aliases
- Production deployment (Docker + Nginx + SSL + Backups)
- CLI with stable commands
- Complete documentation
- **Critical**: Fix 6 security issues

**v0.2.0 (Network Features - 2-3 months)**:
- Network topology system
- Network-aware transfer routing
- Network benchmarking
- Complete CLI network commands
- Enhanced documentation

**v0.3.0 or Separate Release (Web UI - 2-3 months)**:
- Reflex-based Web UI
- Browser-based simulation management
- Visual workflows
- Dashboard and monitoring
- Could be released independently as `tellus-webui` package

**v0.4.0+ (Future)**:
- GraphQL API (Issue #56)
- Authentication & Authorization
- Multi-tenancy
- Monitoring & Observability
- Advanced search and analytics

---

## Critical Path to v0.1.0 Release

### Phase 1: Security Fixes (Week 1)

**CRITICAL** - Must complete before release:

1. ✅ Run `./deploy-preflight.sh` to validate current state
2. ❌ Fix CORS wildcard in `nginx/conf.d/tellus-api.conf`
   - Replace `'*'` with specific origins
   - Document allowed origins in `.env.example`
3. ❌ Create `nginx/ssl/` directory
   - Add placeholder README
   - Update deployment guide
4. ❌ Create `backups/` directory
   - Add placeholder README
   - Verify backup script works
5. ❌ Fix Nginx domain substitution
   - Use `envsubst` or `scripts/setup-nginx-config.sh`
   - Test template processing
6. ❌ Add API authentication
   - Implement API key validation
   - Document in API_REFERENCE.md
7. ❌ Add password validation
   - Enforce strong passwords in `.env.example`
   - Add validation script

**Deliverable**: All preflight checks pass ✅

### Phase 2: Testing (Week 2)

1. **Local Deployment Test**:
   ```bash
   # Test production stack locally
   docker compose -f docker-compose.prod.yml up -d --build

   # Verify all services healthy
   docker compose -f docker-compose.prod.yml ps

   # Run integration tests
   TELLUS_API_URL=http://localhost:1968/api/prep-release pixi run -e test pytest tests/integration/

   # Test backup script
   docker compose -f docker-compose.prod.yml exec postgres-backup /backup.sh
   ```

2. **Staging Deployment**:
   - Deploy to staging server following DEPLOYMENT.md
   - Run through DEPLOYMENT_CHECKLIST.md
   - Load test with realistic data
   - Verify SSL certificates work
   - Test automated backups
   - Monitor for 48 hours

3. **Documentation Review**:
   - Verify all code examples work
   - Test installation instructions
   - Validate API examples
   - Check for broken links

**Deliverable**: Staging deployment passes all checks ✅

### Phase 3: Release (Week 2-3)

1. **Version Update**:
   ```bash
   # Update version in pyproject.toml
   sed -i 's/version = "0.1.0a3"/version = "0.1.0"/' pyproject.toml

   # Update API version if needed
   # (Currently hardcoded to "prep-release" in version.py)

   # Commit version bump
   git add pyproject.toml
   git commit -m "bump: version 0.1.0a3 → 0.1.0"
   ```

2. **Final Documentation Check**:
   - Ensure CHANGELOG.md is complete
   - Update README.md with installation from PyPI
   - Verify all docs reference v0.1.0

3. **Create GitHub Release**:
   ```bash
   # Tag release
   git tag v0.1.0
   git push origin v0.1.0

   # Create GitHub release with release notes
   gh release create v0.1.0 \
     --title "Tellus v0.1.0 - Production-Ready REST API" \
     --notes-file CHANGELOG.md
   ```

4. **Publish to PyPI**:
   ```bash
   # Build package
   pixi run python -m build

   # Upload to PyPI (using trusted publishing)
   # GitHub Actions will handle this automatically on tag push
   ```

5. **Deploy to Production**:
   - Follow DEPLOYMENT.md guide
   - Use DEPLOYMENT_CHECKLIST.md
   - Monitor closely for first 24 hours
   - Have rollback plan ready

6. **Announce Release**:
   - GitHub Discussions post
   - Update project website
   - Notify AWI TerraAI team
   - Update roadmap

**Deliverable**: v0.1.0 tagged, published, and deployed ✅

---

## Post-Release Activities

### Immediate (Days 1-7)
- Monitor production deployment
- Watch for GitHub issues
- Collect user feedback
- Track PyPI download stats
- Address critical bugs immediately

### Short-term (Weeks 2-4)
- Plan v0.1.1 patch release if needed
- Start v0.2.0 planning (network features)
- Begin web-ui integration planning
- Update roadmap based on feedback

### Medium-term (Months 2-3)
- Merge feat/network-aware-transfers to develop branch
- Complete network CLI commands
- Fix network configuration persistence
- Integration test network features
- Prepare v0.2.0 alpha releases

---

## Network Branch Integration Plan (v0.2.0)

**NOT for v0.1.0 - Defer to next release**

### Prerequisites
1. ✅ v0.1.0 released and stable
2. ❌ Network CLI commands completed
3. ❌ Network configuration persistence working
4. ❌ Integration tests passing
5. ❌ Documentation updated

### Merge Strategy
```bash
# After v0.1.0 release, create v0.2.0 development branch
git checkout -b develop/v0.2.0

# Merge network features
git merge origin/feat/network-aware-transfers

# Resolve conflicts (expect conflicts in):
# - pyproject.toml (dependencies)
# - pixi.lock
# - RELEASE_STATUS.md
# - src/tellus/interfaces/cli/simulation.py

# Test thoroughly
pixi run -e test pytest

# Update documentation
# Update CHANGELOG for v0.2.0

# Create alpha releases for testing
git tag v0.2.0a1
```

### Testing Plan
1. Unit tests (should mostly pass)
2. Integration tests for network features
3. CLI command testing
4. Network benchmarking validation
5. Performance testing with realistic data
6. Documentation verification

### Estimated Timeline: 2-3 months after v0.1.0

---

## Web UI Integration Plan (v0.3.0 or Separate)

**NOT for v0.1.0 - Defer to separate release**

### Two Options:

#### Option A: Include in v0.3.0
- Timeline: 4-6 months after v0.1.0
- Requires: v0.2.0 (network features) released first
- Benefits: Single unified release
- Risks: Delays main release cycle

#### Option B: Separate `tellus-webui` Package (RECOMMENDED)
- Timeline: Can release independently
- Package name: `tellus-webui` on PyPI
- Requires: `tellus>=0.1.0` as dependency
- Benefits:
  - Independent release cycle
  - Users can choose CLI-only or CLI+Web
  - Web UI can iterate faster
  - Clearer separation of concerns
- Installation:
  ```bash
  pip install tellus          # CLI + REST API + SDK
  pip install tellus-webui    # Adds Web UI
  ```

### Recommended: Option B (Separate Package)

**Rationale**:
- Web UI has different update cadence than core
- Not all users need web interface
- Keeps core package lean
- Allows web UI to use modern Python (3.11+) while core supports 3.9+
- Easier to maintain and test separately

---

## Decision Summary

### ✅ RECOMMENDED: Minimal v0.1.0 (Option 1)

**Commit to v0.1.0**:
- REST API + Python SDK + Production Deployment
- Fix 6 critical security issues
- Release in 1-2 weeks
- Target: January 27, 2025

**Defer to v0.2.0** (Q2 2025):
- Network topology system
- Network-aware transfers
- Complete network CLI commands

**Defer to Separate Release** (Q2-Q3 2025):
- Web UI as `tellus-webui` package
- Independent release cycle
- Requires `tellus>=0.1.0`

### Why This is the Right Choice

1. **Focused Scope**: Clear feature boundary reduces risk
2. **Faster Time to Market**: 1-2 weeks vs 5-6 weeks
3. **Lower Risk**: All features tested and documented
4. **Production Ready**: Infrastructure is solid after fixes
5. **Clear Upgrade Path**: Users know what's coming next
6. **Flexibility**: Network and Web UI can iterate independently

### Next Actions

1. **User Decision Required**: Approve minimal v0.1.0 strategy
2. **Start Week 1**: Fix critical security issues
3. **Start Week 2**: Test and deploy to staging
4. **Week 3**: Tag, release, and announce v0.1.0
5. **Post-release**: Begin v0.2.0 planning

---

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|------------|--------|------------|
| Security fixes break deployment | Low | High | Thorough testing in staging |
| Integration issues in staging | Medium | Medium | Have rollback plan ready |
| Documentation gaps | Low | Low | User feedback loop |
| Performance issues | Low | Medium | Load testing before production |
| Network features delay v0.2.0 | Medium | Low | Independent timeline |
| Web UI integration complex | Medium | Low | Separate package reduces risk |

**Overall Risk Level**: **LOW** for minimal v0.1.0 approach

---

## Conclusion

**Recommendation**: Proceed with **Option 1 - Minimal v0.1.0**

The prep-release branch is documentation-complete and code-complete for the core features. After fixing the 6 critical security issues (1-2 weeks), we'll have a production-ready v0.1.0 release that provides solid value:

- ✅ Complete REST API for programmatic access
- ✅ Clean Python SDK for library usage
- ✅ Production deployment infrastructure
- ✅ Comprehensive documentation

Network features and Web UI are valuable additions but should not delay the core release. They can be delivered in subsequent releases (v0.2.0, v0.3.0, or separately) with proper testing and integration.

**Target Release Date**: January 27, 2025 (2 weeks from today)

**Release Confidence**: High (after security fixes)
