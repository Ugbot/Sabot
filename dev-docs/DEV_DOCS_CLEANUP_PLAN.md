# Dev-Docs Cleanup Plan

**Date:** October 8, 2025
**Status:** Analysis Complete

---

## Overview

The `dev-docs/` directory contains 97 markdown files tracking implementation progress, design decisions, and technical discoveries. Many documents contain outdated information that needs updating.

---

## Current Structure

```
dev-docs/
├── implementation/     # 31 files - Phase 1-9 implementation guides
├── results/           # 17 files - Test results, benchmarks
├── status/            # 16 files - Implementation status reports
├── design/            # 9 files - Design documents
├── planning/          # 5 files - Roadmaps, implementation plans
├── roadmap/           # 5 files - Feature roadmaps
├── analysis/          # 2 files - Code analysis, stub inventory
├── testing/           # 1 file - Test organization
└── *.md               # 11 root-level files
```

**Total:** 97 markdown files

---

## Issues Found

### 1. Outdated State Backend References (4 files)

**Files with incorrect "Tonbo/RocksDB" information:**
- `dev-docs/status/IMPLEMENTATION_COMPLETE.md`
- `dev-docs/design/CYTHON_OPERATORS_SUMMARY.md`
- `dev-docs/results/IMPLEMENTATION_SUMMARY.md`
- `dev-docs/status/ACTUAL_STATUS_TESTED.md`

**Problem:**
- References to "Tonbo/RocksDB integration pending"
- Implies they are a single backend to be integrated
- Says "Smart backend selection ready" but not implemented

**Actual status:**
- Tonbo and RocksDB are BOTH implemented and active
- Used automatically for different purposes (data vs metadata)
- Not pending - fully integrated

### 2. Broken Cross-References (10+ files)

**Files referencing documents that moved:**
- Reference `planning/REALITY_CHECK.md` (was `../REALITY_CHECK.md`)
- Reference `analysis/STUB_INVENTORY.md` (was `../STUB_INVENTORY.md`)
- Reference `../PROJECT_MAP.md` (correct - still in root)

**Affected files:**
- `dev-docs/INDEX.md`
- `dev-docs/planning/IMPLEMENTATION_PLAN.md`
- `dev-docs/roadmap/*.md` (5 files)
- `dev-docs/analysis/*.md` (2 files)

### 3. INDEX.md Outdated (1 file)

**`dev-docs/INDEX.md` issues:**
- Line 7: References `planning/REALITY_CHECK.md` (was `../REALITY_CHECK.md`) ✅ FIXED
- Line 9: References `analysis/STUB_INVENTORY.md` (was `../STUB_INVENTORY.md`) ✅ FIXED
- Line 153: Still says "incomplete features" when many are now complete
- Missing `implementation/` and `testing/` subdirectories
- "Last Updated: October 2, 2025" is 6 days old

### 4. Root-Level Files Not Organized

**11 files in dev-docs root that should be in subdirectories:**
```
dev-docs/
├── AGENT_WORKER_MODEL.md           → design/
├── ARROW_MIGRATION.md              → implementation/
├── ARROW_XXH3_HASH_INTEGRATION.md  → implementation/
├── CYARROW.md                      → implementation/
├── DATA_FORMATS.md                 → design/
├── DBOS_WORKFLOWS.md               → implementation/
├── DEMO_QUICKSTART.md              → results/
├── IMPLEMENTATION_ROADMAP.md       → planning/
├── LOCK_FREE_QUEUE_OPTIMIZATION.md → implementation/
├── PERFORMANCE_SUMMARY.md          → results/
└── INDEX.md                        → Keep in root
```

### 5. Duplicate/Overlapping Status Files

**Status files with overlapping content:**
- `status/IMPLEMENTATION_STATUS.md`
- `status/IMPLEMENTATION_COMPLETE.md`
- `status/ACTUAL_STATUS_TESTED.md`
- `status/COMPLETE_STATUS_AUDIT_OCT2025.md`
- `results/IMPLEMENTATION_SUMMARY.md`
- `results/IMPLEMENTATION_DEEP_DIVE.md`

**Need to determine:**
- Which is the "source of truth"?
- Can we consolidate or clearly differentiate?

---

## Cleanup Actions

### Phase 1: Fix State Backend References

Update 4 files to reflect actual hybrid storage architecture:

1. **`status/IMPLEMENTATION_COMPLETE.md`**
   - Change "Tonbo/RocksDB integration pending" → "Tonbo/RocksDB hybrid storage active"
   - Update "Smart backend selection ready" → "Smart backend selection implemented"

2. **`design/CYTHON_OPERATORS_SUMMARY.md`**
   - Update state backend section to show both are active
   - Remove "pending integration" language

3. **`results/IMPLEMENTATION_SUMMARY.md`**
   - Update state backend references
   - Clarify automatic usage

4. **`status/ACTUAL_STATUS_TESTED.md`**
   - Mark state backends as "implemented" not "pending"

### Phase 2: Fix Cross-References

Update 10 files with correct paths:

**Pattern to find and replace:**
```bash
# Find all broken references
grep -r "\.\./REALITY_CHECK\.md" dev-docs/
grep -r "\.\./STUB_INVENTORY\.md" dev-docs/

# Replace with:
../REALITY_CHECK.md → planning/REALITY_CHECK.md ✅ FIXED
../STUB_INVENTORY.md → analysis/STUB_INVENTORY.md ✅ FIXED
```

**Files to update:**
- `INDEX.md`
- `planning/IMPLEMENTATION_PLAN.md`
- `roadmap/DEVELOPMENT_ROADMAP.md`
- `roadmap/MISSING_ARROW_FEATURES.md`
- `roadmap/NEXT_IMPLEMENTATION_GUIDE.md`
- `roadmap/FLINK_PARITY_ROADMAP.md`
- `roadmap/FLINK_PARITY_CYTHON_ROADMAP.md`
- `analysis/CURRENT_STUB_ANALYSIS.md`

### Phase 3: Reorganize Root-Level Files

Move 10 files to appropriate subdirectories:

```bash
# Design documents
mv dev-docs/AGENT_WORKER_MODEL.md dev-docs/design/
mv dev-docs/DATA_FORMATS.md dev-docs/design/

# Implementation guides
mv dev-docs/ARROW_MIGRATION.md dev-docs/implementation/
mv dev-docs/ARROW_XXH3_HASH_INTEGRATION.md dev-docs/implementation/
mv dev-docs/CYARROW.md dev-docs/implementation/
mv dev-docs/DBOS_WORKFLOWS.md dev-docs/implementation/
mv dev-docs/LOCK_FREE_QUEUE_OPTIMIZATION.md dev-docs/implementation/

# Results
mv dev-docs/DEMO_QUICKSTART.md dev-docs/results/
mv dev-docs/PERFORMANCE_SUMMARY.md dev-docs/results/

# Planning
mv dev-docs/IMPLEMENTATION_ROADMAP.md dev-docs/planning/
```

### Phase 4: Update INDEX.md

Rewrite `dev-docs/INDEX.md` to:
- Fix broken cross-references
- Add missing subdirectories (implementation/, testing/)
- Update "Last Updated" date
- Add state backend status (implemented, not pending)
- Reorganize to show correct file locations

### Phase 5: Add Missing Subdirectory READMEs

Create README.md files for subdirectories that lack them:

```bash
# Need READMEs
dev-docs/implementation/README.md (exists - check if current)
dev-docs/results/README.md (missing)
dev-docs/status/README.md (missing)
dev-docs/design/README.md (missing)
dev-docs/planning/README.md (missing)
dev-docs/roadmap/README.md (missing)
dev-docs/analysis/README.md (missing)
dev-docs/testing/README.md (missing)
```

---

## Priority

**P0 - Critical (affects accuracy):**
- Phase 1: Fix state backend references (4 files)
- Phase 2: Fix broken cross-references (10 files)

**P1 - High (improves navigation):**
- Phase 4: Update INDEX.md (1 file)

**P2 - Medium (improves organization):**
- Phase 3: Reorganize root-level files (10 files)

**P3 - Low (nice to have):**
- Phase 5: Add subdirectory READMEs (8 files)

---

## Implementation Plan

### Step 1: State Backend References (15 minutes)

```bash
# Update 4 files with correct state backend info
# Replace "Tonbo/RocksDB integration pending" language
# Add hybrid storage architecture description
```

### Step 2: Cross-References (10 minutes)

```bash
# Find and replace broken paths
sed -i '' 's|../REALITY_CHECK.md|planning/REALITY_CHECK.md|g' dev-docs/**/*.md  # ✅ DONE
sed -i '' 's|../STUB_INVENTORY.md|analysis/STUB_INVENTORY.md|g' dev-docs/**/*.md  # ✅ DONE
```

### Step 3: Root-Level Files (5 minutes)

```bash
# Move 10 files to subdirectories
# Update any internal cross-references
```

### Step 4: INDEX.md (20 minutes)

```bash
# Rewrite with:
# - Correct directory structure
# - Fixed cross-references
# - Current status
# - All subdirectories listed
```

### Step 5: Subdirectory READMEs (optional, 30 minutes)

```bash
# Create 8 README.md files
# Each with:
# - Purpose of subdirectory
# - File listing
# - Navigation links
```

---

## Verification

After cleanup:

```bash
# 1. Check for broken links
find dev-docs/ -name "*.md" -exec grep -l "\.\./\(REALITY_CHECK\|STUB_INVENTORY\)\.md" {} \;
# Should return: 0 files

# 2. Check for outdated state backend references
grep -r "Tonbo/RocksDB integration pending" dev-docs/
# Should return: 0 matches

# 3. Check root-level files
ls dev-docs/*.md
# Should return: Only INDEX.md

# 4. Verify all subdirectories have READMEs
find dev-docs/ -mindepth 1 -maxdepth 1 -type d -exec test -f {}/README.md \; -print
# Should list all subdirectories
```

---

## Expected Outcome

**Before:**
- 97 files, many with outdated info
- 4 files with incorrect state backend references
- 10+ files with broken cross-references
- 11 root-level files (should be in subdirectories)
- INDEX.md out of date

**After:**
- 97 files (same count, reorganized)
- All state backend references accurate
- All cross-references working
- Only INDEX.md in root
- INDEX.md current and accurate
- Optional: 8 new subdirectory READMEs (105 total)

---

## Risk Assessment

**Low Risk:**
- Phase 1-2: Text replacements only, no file moves
- Phase 4: Single file update

**Medium Risk:**
- Phase 3: File moves (10 files) - could break external references

**Mitigation:**
- Create symlinks from old locations to new locations
- Or add "MOVED" redirect files
- Update any external docs that reference dev-docs/

---

## Timeline

**Minimal cleanup (P0-P1):** 45 minutes
- Phase 1: 15 min
- Phase 2: 10 min
- Phase 4: 20 min

**Full cleanup (P0-P2):** 50 minutes
- Add Phase 3: +5 min

**Complete with READMEs (P0-P3):** 80 minutes
- Add Phase 5: +30 min

---

## Next Actions

1. **Review this plan** - Confirm approach
2. **Execute P0 (Phase 1-2)** - Fix accuracy issues
3. **Execute P1 (Phase 4)** - Update INDEX.md
4. **Optional: Execute P2-P3** - Full reorganization

---

## Notes

- All changes are to dev-docs/ only (internal documentation)
- User-facing docs (docs/, examples/) already cleaned up
- Main PROJECT_MAP.md already updated with correct state backend info
- This cleanup makes dev-docs/ consistent with main docs

---

**Created:** October 8, 2025
**Status:** Ready for execution
**Estimated Time:** 45-80 minutes depending on scope
