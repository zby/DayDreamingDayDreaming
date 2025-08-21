#!/bin/bash
# scripts/rebuild_results.sh
# Rebuild cross-experiment tracking tables from existing files (two-phase + legacy)
#
# Usage:
#   ./scripts/rebuild_results.sh
#   (or) bash scripts/rebuild_results.sh
#
# What it does:
# - Clears existing cross-experiment tables (generation: links/essays/legacy; evaluation)
# - Ensures the target directory exists
# - Rebuilds generation tables via scripts/build_generation_results_table.py
# - Rebuilds evaluation table via scripts/build_evaluation_results_table.py

set -euo pipefail

echo "🚀 Rebuilding cross-experiment results from existing responses..."

# Create directory if it doesn't exist
mkdir -p data/7_cross_experiment

# Remove existing tables
echo "🧹 Clearing old tables (if any)"
rm -f \
  data/7_cross_experiment/link_generation_results.csv \
  data/7_cross_experiment/essay_generation_results.csv \
  data/7_cross_experiment/generation_results.csv \
  data/7_cross_experiment/evaluation_results.csv || true

# Build generation tables (two-phase preferred, legacy supported)
echo "📊 Rebuilding generation tables (links + essays, and legacy if present)..."
python scripts/build_generation_results_table.py

# Build evaluation table (two-phase preferred, legacy fallback)
echo "📊 Rebuilding evaluation table..."
python scripts/build_evaluation_results_table.py

# Parse scores across all evaluation responses into 7_cross_experiment
echo "🧮 Parsing evaluation scores (cross-experiment)..."
python scripts/parse_all_scores.py --output data/7_cross_experiment/parsed_scores.csv

# Build pivot tables from parsed scores
echo "📈 Building pivot tables..."
python scripts/build_pivot_tables.py --parsed-scores data/7_cross_experiment/parsed_scores.csv

echo "✅ Cross-experiment tables and pivots rebuilt successfully"
