# REFactor Notes

## 2025-02-14
- Removed `src/daydreaming_dagster/constants.py`; import `STAGES` from `src/daydreaming_dagster/types.py` and generation filenames via `src/daydreaming_dagster/data_layer/paths.py` (`GEN_ARTIFACT_FILENAMES`).
- No compatibility shim left in place; external consumers expecting `daydreaming_dagster.constants.GEN_FILES` must migrate to `data_layer.paths` helpers.

## 2025-02-15
- Deleted the unused `evaluator_agreement_analysis` asset and its CSV target (`analysis/evaluator_agreement.csv`). Cohort analysis now relies solely on `comprehensive_variance_analysis` for multi-evaluator variance reporting.
