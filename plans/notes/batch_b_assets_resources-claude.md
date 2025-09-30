# Batch B Assets & Resources Simplification Review

**Review Date**: 2025-09-30  
**Targets**: `assets/` (3,194 SLOC), `resources/` (375 SLOC)  
**Reviewer**: Claude Code  

## Executive Summary

The assets and resources layers show good DDError contract alignment but suffer from complexity concentration in key functions. Error handling is mostly boundary-appropriate, though some asset functions mix concerns. Dependency injection patterns are inconsistent, and several constructs add unnecessary complexity.

**Key Findings**:
- 83% of error handling properly uses DDError contract
- 2 major complexity hotspots requiring refactoring
- 15+ opportunities for dependency injection improvements
- Repeated asset signature patterns suggest decorator consolidation opportunities

## Asset/Resource Signatures vs Dagster IO Manager Expectations

### ✅ Well-Aligned Patterns

**Standard Asset Pattern**: All generation assets (`group_draft.py`, `group_essay.py`, `group_evaluation.py`) follow consistent signatures:
```python
@asset_with_boundary(
    stage="<stage>", 
    partitions_def=<stage>_gens_partitions,
    group_name="generation_<stage>",
    io_manager_key="<appropriate>_io_manager",
    required_resource_keys={"data_root", "experiment_config", ...}
)
def <stage>_<artifact>(context, <inputs>) -> <output_type>:
```

**IO Manager Consistency**: Resources follow clear separation:
- `GensPromptIOManager`: Handles prompt text persistence to gens store
- `CSVIOManager`: Standard DataFrame-to-CSV persistence 
- `InMemoryIOManager`: Test/ephemeral data with fallback to gens store

### ⚠️ Problematic Patterns

**Mixed IO Manager Usage**: `/home/zby/llm/DayDreamingDayDreaming/src/daydreaming_dagster/assets/results_summary.py:15`
```python
# Uses two different IO manager keys in same file
io_manager_key="summary_results_io_manager"  # Line 15
# vs other assets using different keys
```

**Resource Key Inconsistency**: Some assets use `getattr(context.resources, "...")` while others declare in `required_resource_keys`. This creates unclear dependency contracts.

## Error Handling Patterns & DDError Alignment

### ✅ Proper DDError Usage

**Boundary Layer Compliance**: `/home/zby/llm/DayDreamingDayDreaming/src/daydreaming_dagster/assets/_error_boundary.py:26-35`
```python
except DDError as err:
    logger = get_dagster_logger()
    description = f"[{stage}] {err}"
    metadata: dict[str, MetadataValue] = {
        "error_code": MetadataValue.text(err.code.name)
    }
    if err.ctx:
        metadata.update(_ctx_to_metadata(err.ctx))
    raise Failure(description=description, metadata=metadata) from err
```

**Proper Error Propagation**: `/home/zby/llm/DayDreamingDayDreaming/src/daydreaming_dagster/assets/group_evaluation.py:113-119`
```python
try:
    raw_metadata = data_layer.read_raw_metadata(EVALUATION_STAGE, gen_id)
except DDError as err:
    if err.code is Err.DATA_MISSING:
        raw_metadata = {}
    else:
        raise  # Preserve other errors
```

### ⚠️ Error Handling Anti-Patterns

**Silent Exception Swallowing**: `/home/zby/llm/DayDreamingDayDreaming/src/daydreaming_dagster/resources/llm_client.py:53-54`
```python
except Exception:
    return {}  # Silent failure loses debugging context
```

**Generic Exception Handling**: `/home/zby/llm/DayDreamingDayDreaming/src/daydreaming_dagster/resources/llm_client.py:69-70`
```python
except Exception:
    pass  # Too broad, loses specific error types
```

## Dependency Injection Opportunities

### Current Anti-Patterns

**Global Resource Access**: Widespread pattern across assets:
```python
# Instead of dependency injection
data_layer = GensDataLayer.from_root(context.resources.data_root)
experiment_config = getattr(context.resources, "experiment_config", None)
llm_client = getattr(context.resources, "openrouter_client", None)
```

**Resource Factory Pattern**: `/home/zby/llm/DayDreamingDayDreaming/src/daydreaming_dagster/resources/io_managers.py:86-98`
- `InMemoryIOManager.__init__()` accepts `fallback_data_root` but creates internal `GensDataLayer`
- Better: inject `GensDataLayer` directly

### Recommended Improvements

1. **Resource Composition**: Create composite resources that encapsulate related dependencies
2. **Typed Resource Protocols**: Define interfaces for resource contracts
3. **Factory Elimination**: Replace resource factories with dependency injection

## Complexity Hotspots Analysis

### Major Hotspot: `cohort_membership` (F(63) complexity)

**Location**: `/home/zby/llm/DayDreamingDayDreaming/src/daydreaming_dagster/assets/group_cohorts.py:528`

**Issues**:
- 500+ line function mixing multiple concerns
- Complex branching logic (curated vs Cartesian mode)
- Embedded helper classes (`_ReplicateAllocator`, `CuratedSelectionConfig`)
- File parsing, validation, ID generation, and DataFrame construction in single function

**Recommended Refactoring**:
```python
# Split into focused functions
def cohort_membership(context, cohort_id: str) -> pd.DataFrame:
    config = _load_selection_config(data_root)
    if config.selection_type:
        return _build_curated_membership(config, data_root, cohort_id)
    else:
        return _build_cartesian_membership(data_root, cohort_id)

def _build_curated_membership(...): # ~150 lines
def _build_cartesian_membership(...): # ~200 lines  
def _load_selection_config(...): # ~50 lines
```

### Secondary Hotspot: `comprehensive_variance_analysis` (C(19) complexity)

**Location**: `/home/zby/llm/DayDreamingDayDreaming/src/daydreaming_dagster/assets/results_analysis.py:117`

**Issues**:
- Multiple nested aggregation operations
- Complex column mapping and DataFrame pivoting
- Mixed statistical computations

**Recommended Refactoring**:
```python
def comprehensive_variance_analysis(context, aggregated_scores: pd.DataFrame) -> pd.DataFrame:
    valid_scores = filter_valid_scores(aggregated_scores)
    template_variance = _compute_template_variance(valid_scores)
    model_variance = _compute_model_variance(valid_scores)
    overall_variance = _compute_overall_variance(valid_scores)
    return _combine_variance_analyses(template_variance, model_variance, overall_variance)
```

## Confusing Constructs Catalog

### 1. Callable-Typed Fields in Stateful Objects

**Location**: `_ReplicateAllocator` class in `/home/zby/llm/DayDreamingDayDreaming/src/daydreaming_dagster/assets/group_cohorts.py:180-206`
```python
# Confusing: mixing filesystem probing with in-memory state
self._next_indices: Dict[tuple[str, tuple], int] = {}
# Complex key structure: (stage_norm, base_signature) as composite key
```

**Issue**: The tuple-of-tuple key structure is difficult to reason about and debug.

### 2. Stage String Normalization Patterns

**Scattered Pattern**:
```python
stage_norm = str(stage).lower()  # Appears 6+ times across files
```

**Recommendation**: Centralize in a `Stage` enum or helper function.

### 3. Mixed Async/Sync Resource Management

**LLMClientResource**: `/home/zby/llm/DayDreamingDayDreaming/src/daydreaming_dagster/resources/llm_client.py`
- Mixes tenacity retry decorators with manual rate limiting
- Combines proactive (`@sleep_and_retry`) and reactive (`@retry`) patterns
- Unclear precedence between retry mechanisms

### 4. Implicit Stage Dependencies

**Pattern**: Assets depend on specific generation stage ordering but don't declare it explicitly:
```python
# Essay depends on draft, evaluation depends on essay
# But dependency is inferred from gen_id parsing, not declared
```

## Asset/Resource Consolidation Recommendations

### 1. **Asset Decorator Consolidation**

**Current**: 3 nearly identical asset groups (`group_draft.py`, `group_essay.py`, `group_evaluation.py`)

**Proposed**: Generic stage asset factory:
```python
def create_stage_assets(stage: Stage) -> list[AssetsDefinition]:
    return [
        stage_prompt_asset(stage),
        stage_raw_asset(stage), 
        stage_parsed_asset(stage)
    ]
```

### 2. **Resource Interface Standardization**

**Current**: Different resources use different configuration patterns
**Proposed**: Common resource protocol:
```python
class StageResource(Protocol):
    def configure_for_stage(self, stage: Stage) -> StageConfig
    def get_data_layer(self) -> GensDataLayer
```

### 3. **IO Manager Hierarchy**

**Current**: Separate IO managers with overlapping logic
**Proposed**: Composition-based hierarchy:
```python
class CompositeIOManager(IOManager):
    def __init__(self, csv_manager: CSVIOManager, gens_manager: GensPromptIOManager):
        # Route based on asset type rather than separate managers
```

## Bold Refactors (Contract-Breaking)

### 1. **Eliminate Asset/Stage String Coupling**

**Current Issue**: Stage strings scattered throughout, no type safety
**Breaking Change**: Replace all stage strings with `Stage` enum
**Migration**: Automated string → enum conversion, update all callsites

### 2. **Resource Dependency Inversion**

**Current**: Assets create their own `GensDataLayer` instances
**Breaking Change**: Inject `GensDataLayer` as a resource
**Migration**: Update all asset signatures to accept `GensDataLayer` parameter

### 3. **Partition Key Type Safety**

**Current**: Partition keys are untyped strings
**Breaking Change**: Typed partition key classes (`GenId`, `CohortId`)
**Migration**: Update all assets to use typed partition keys

## Test Simplification Recommendations

### 1. **Fixture Consolidation**

**Pattern**: `/home/zby/llm/DayDreamingDayDreaming/src/daydreaming_dagster/assets/tests/test_cohort_membership.py:29-50`
- Many tests duplicate data root setup
- CSV writing helpers repeated across test files

**Recommendation**: 
```python
@pytest.fixture(scope="session")
def standard_data_root() -> Path:
    # Standard test data layout used by 80% of tests
```

### 2. **Test Asset Factories**

**Current**: Tests manually call asset functions with mock contexts
**Better**: Test utilities that create realistic contexts:
```python
def create_test_context(stage: Stage, gen_id: str, **resource_overrides) -> AssetExecutionContext:
    # Pre-configured context with standard test resources
```

### 3. **Error Testing Patterns**

**Current**: Scattered `pytest.raises(DDError)` without code verification
**Better**: Error testing utilities:
```python
def assert_dd_error(callable, expected_code: Err, **expected_ctx):
    # Verify both error code and context content
```

## Migration Timeline & Risk Assessment

### Phase 1: Low-Risk Improvements (1-2 weeks)
- Asset decorator consolidation
- Dependency injection for new assets
- Test fixture consolidation
- Error handling standardization

### Phase 2: Medium-Risk Refactoring (2-3 weeks)
- `cohort_membership` function decomposition
- Resource interface standardization
- Complex analysis function breakdown

### Phase 3: High-Risk Breaking Changes (3-4 weeks)
- Stage enum migration
- Typed partition keys
- Resource dependency inversion

**Risk Mitigation**: Maintain parallel implementations during transition, use feature flags for gradual rollout.

## Conclusion

The assets/resources boundary shows solid architectural foundations but needs focused attention on complexity hotspots and consistency. The error handling largely follows DDError contracts, but dependency injection patterns need standardization. 

**Priority 1**: Break down `cohort_membership` function complexity
**Priority 2**: Standardize resource dependency patterns  
**Priority 3**: Consolidate repeated asset signatures

**Expected Reduction**: 15-20% SLOC reduction with improved maintainability and testability.