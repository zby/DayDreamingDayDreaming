# Batch C - Data Layer Foundations Simplification Review

**Target**: `/src/daydreaming_dagster/data_layer/` (368 SLOC actual vs 361 baseline)
**Review Date**: 2025-09-30

## Executive Summary

The data layer components are surprisingly clean and well-structured. Current implementation already enforces `Paths` usage consistently and has structured error handling. The primary opportunities are in consolidating redundant methods and addressing scattered path usage across the broader codebase.

## Current Architecture Assessment

### Strengths
- **Clean separation**: `Paths` provides single source of truth for path construction
- **Structured errors**: Consistent `DDError` usage throughout serialization/deserialization
- **Good abstractions**: `GensDataLayer` provides clean facade over filesystem operations
- **Type safety**: Proper dataclass usage for `GenerationMetadata`

### Key Files Analysis

#### `/src/daydreaming_dagster/data_layer/paths.py` (150 SLOC)
- **Status**: Well-designed, minimal issues
- **Strengths**: Canonical filenames as constants, frozen dataclass, comprehensive path helpers
- **Minor issues**: `__post_init__` validation could be streamlined

#### `/src/daydreaming_dagster/data_layer/gens_data_layer.py` (277 SLOC)  
- **Status**: Functional but verbose
- **Strengths**: Consistent error handling, proper path delegation to `Paths`
- **Issues**: Significant method duplication in read/write operations

#### `/tests/data_layer/test_gens_data_layer.py` (142 SLOC)
- **Status**: Good coverage, could be more concise
- **Strengths**: Tests structured error scenarios properly
- **Issues**: Some test duplication, helper function could be extracted

## Critical Issues Found

### 1. **Path Usage Violations Across Codebase** (HIGH PRIORITY)
**Problem**: Despite having `Paths` helpers, 47 locations still use direct string concatenation with `data_root`

**Examples**:
```python
# In llm_client.py
csv_path = Path(self.data_root) / "1_raw" / "llm_models.csv"  # Should use paths.llm_models_csv

# In cohort_scope.py  
csv_path = self._data_root / "cohorts" / cohort_id / "membership.csv"  # Should use paths.cohort_membership_csv

# In group_cohorts.py
essays_path = data_root / "2_tasks" / "selected_essays.txt"  # Should use paths method
```

**Impact**: Violates single source of truth principle, creates maintenance burden

### 2. **Method Duplication in GensDataLayer** (MEDIUM PRIORITY)
**Problem**: `write_*` and `read_*` methods follow identical patterns with only target path differences

**Current Pattern**:
```python
def write_raw(self, stage: str, gen_id: str, text: str) -> Path:
    target = self._paths.raw_path(stage, gen_id)
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        target.write_text(str(text or ""), encoding="utf-8")
    except OSError as exc:
        raise DDError(Err.IO_ERROR, ctx={"path": str(target)}) from exc
    return target

def write_parsed(self, stage: str, gen_id: str, text: str) -> Path:
    # Identical implementation with different target path
```

**Suggested Consolidation**:
```python
def _write_text(self, target: Path, text: str) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        target.write_text(str(text or ""), encoding="utf-8")
    except OSError as exc:
        raise DDError(Err.IO_ERROR, ctx={"path": str(target)}) from exc
    return target

def write_raw(self, stage: str, gen_id: str, text: str) -> Path:
    return self._write_text(self._paths.raw_path(stage, gen_id), text)
```

### 3. **JSON Serialization Duplication** (MEDIUM PRIORITY)  
**Problem**: Three near-identical metadata write methods

**Recommended**: Extract `_write_json(target: Path, data: Dict[str, Any])` helper

## Prioritized Cleanup Recommendations

### Tier 1: High-Impact Schema Rewrites (Even if breaking consumers)

1. **Enforce Paths Usage Codebase-Wide** 
   - **Action**: Replace all 47 direct path concatenations with `Paths` method calls
   - **Breaking**: May require adding new `Paths` methods for currently hard-coded paths
   - **Benefit**: True single source of truth, easier refactoring
   - **Risk**: Medium - affects many files, but changes are mechanical

2. **Consolidate GensDataLayer Methods**
   - **Action**: Extract `_write_text`, `_read_text`, `_write_json`, `_read_json` helpers  
   - **Breaking**: None - internal refactor only
   - **Benefit**: ~40% SLOC reduction in data layer
   - **Risk**: Low - pure refactor

### Tier 2: Confusing Patterns

3. **Hybrid Functional/Object APIs** 
   - **Pattern**: `resolve_generation_metadata(layer, stage, gen_id)` vs `layer.read_main_metadata(stage, gen_id)`
   - **Issue**: Inconsistent - some operations are methods, others are functions
   - **Recommendation**: Move `resolve_generation_metadata` to be `layer.resolve_metadata()` method
   - **Risk**: Low - single function move

### Tier 3: Test Simplification

4. **Test Helper Extraction**
   - **Action**: Extract `_write_metadata` helper to test utilities
   - **Benefit**: Reusable across test files, reduced duplication
   - **Risk**: Minimal

5. **Test Consolidation** 
   - **Action**: Combine round-trip tests that cover similar patterns
   - **Benefit**: Faster test execution, clearer test intent
   - **Risk**: Minimal

## IO Primitives Assessment

### Pure Function Candidates
- `resolve_generation_metadata()` - already pure, just takes dependencies as params
- Path construction methods in `Paths` - all pure property accessors

### Memoization Candidates  
- **Not recommended**: File I/O operations should not be memoized due to external mutation
- **Path objects**: Already cached via `@property` where appropriate

## Validation Strategy

### Current State
- Input validation exists in `Paths.__post_init__()` and `GensDataLayer.__init__()`
- Error contexts are well-structured with proper codes

### Improvement Opportunities  
- **Simplify validation**: `Paths.__post_init__()` validation could be streamlined:
  ```python
  def __post_init__(self):
      if not self.data_root or not str(self.data_root).strip():
          raise DDError(Err.INVALID_CONFIG, ctx={"reason": "paths_missing_data_root"})
      object.__setattr__(self, "data_root", Path(self.data_root))
  ```

## Risk Assessment

| Recommendation | Implementation Risk | Breaking Risk | Confidence |
|----------------|-------------------|---------------|------------|
| Enforce Paths usage | Medium | Medium | High |
| Consolidate GensDataLayer | Low | None | High |  
| Fix hybrid API patterns | Low | Low | Medium |
| Test simplification | Minimal | None | High |

## Metrics Impact Projection

- **Current**: 368 SLOC
- **Post-consolidation**: ~260 SLOC (-29%)
- **Complexity**: Significant reduction due to DRY improvements
- **Maintainability**: High improvement from single source of truth enforcement

## Implementation Notes

1. **Path enforcement** should be done as a separate commit to isolate breaking changes
2. **Method consolidation** can be done incrementally without breaking existing APIs
3. **Test changes** should be bundled with functional changes they support
4. Preserve all existing error codes and context structure per `refactor_contract.yaml`

## Conclusion

The data layer is already well-architected. The highest value improvements focus on DRY principles and enforcing the established patterns consistently across the broader codebase rather than fundamental architectural changes.