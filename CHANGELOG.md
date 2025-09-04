## v0.1.0a3 (2025-09-04)

### Feat

- add new test suite based on CLI specification

### Fix

- clarify THO/SAO variables are MPIOM (not FESOM) ocean variables

## v0.1.0a2 (2025-09-04)

### Feat

- add commitizen and pixi release workflows for conventional commits
- add automated CI/CD publishing workflows

### Fix

- remove empty with block in publish workflow
- comment out animavox dependency to fix GitHub Actions
- use frozen flag in GitHub Actions to resolve lock file platform issues
- regenerate complete pixi lock file
- update pixi lock file for build dependencies and alpha-only version bumps
- configure commitizen to always produce alpha releases
- move release tasks to correct pixi section

## v0.1.0a1 (2025-09-03)

### Feat

- Add async filesystem representation generation and optional column for location list

### Fix

- Separate SSH connection parameters from filesystem path in location.fs
- Add missing type annotation and change command name from ls to list
- Remove --fixed flag and make improved filesystem representation the default
- Address code review recommendations by integrating fixes into existing architecture
- Add ScoutFS protocol support to location creation wizard
