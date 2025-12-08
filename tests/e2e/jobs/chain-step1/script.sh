#!/bin/sh
set -e

echo "=== Chain Step 1: Data Processing ==="
echo "Job: chain.step1"
date -u +%Y-%m-%dT%H:%M:%SZ

# Read input from external source
IN_DIR="${LJC_IN:-$LINEARJC_IN_DIR}"
OUT_DIR="${LJC_OUT:-$LINEARJC_OUT_DIR}"

echo "Input directory: $IN_DIR"
echo "Output directory: $OUT_DIR"

# Read external input
INPUT_FILE="${IN_DIR}/chain_external_input/data.txt"
if [ -f "$INPUT_FILE" ]; then
    echo "Reading input from: $INPUT_FILE"
    INPUT_DATA=$(cat "$INPUT_FILE")
    echo "Input data: $INPUT_DATA"
else
    echo "ERROR: Input file not found: $INPUT_FILE"
    exit 1
fi

# Process data and write to intermediate output
OUTPUT_DIR="${OUT_DIR}/chain_intermediate"
mkdir -p "$OUTPUT_DIR"

echo "Writing intermediate output to: $OUTPUT_DIR"
echo "Step1 processed: ${INPUT_DATA}" > "$OUTPUT_DIR/processed.txt"
echo "Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$OUTPUT_DIR/processed.txt"

echo "=== Chain Step 1 Complete ==="
