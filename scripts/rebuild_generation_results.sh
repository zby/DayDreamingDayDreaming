#!/bin/bash
# scripts/rebuild_generation_results.sh
# Rebuild the cross-experiment generation_results.csv table from existing
# single-phase generation responses.
#
# Usage:
#   ./scripts/rebuild_generation_results.sh
#
# What it does:
# - Removes existing data/7_cross_experiment/generation_results.csv (if present)
# - Ensures the target directory exists
# - Invokes scripts/build_generation_results_table.py to scan legacy responses

echo "🚀 Rebuilding generation_results.csv from existing data..."

# Remove existing table
if [ -f "data/7_cross_experiment/generation_results.csv" ]; then
    echo "🗑️  Removing existing generation_results.csv"
    rm -f data/7_cross_experiment/generation_results.csv
fi

# Create directory if it doesn't exist
mkdir -p data/7_cross_experiment

# Use Python script to scan all existing generation responses
echo "📊 Building table from response files..."
python scripts/build_generation_results_table.py

if [ $? -eq 0 ]; then
    echo "✅ generation_results.csv rebuilt successfully"
else
    echo "❌ Failed to rebuild generation_results.csv"
    exit 1
fi
