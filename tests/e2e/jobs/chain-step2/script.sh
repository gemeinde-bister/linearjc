#!/bin/sh
set -e

echo "=== Chain Step 2: Final Processing ==="
echo "Job: chain.step2"
date -u +%Y-%m-%dT%H:%M:%SZ

# Read intermediate data from step1
IN_DIR="${LJC_IN:-$LINEARJC_IN_DIR}"
OUT_DIR="${LJC_OUT:-$LINEARJC_OUT_DIR}"

echo "Input directory: $IN_DIR"
echo "Output directory: $OUT_DIR"

# Read intermediate input (from step1's output)
INPUT_FILE="${IN_DIR}/chain_intermediate/processed.txt"
if [ -f "$INPUT_FILE" ]; then
    echo "Reading intermediate data from: $INPUT_FILE"
    INTERMEDIATE_DATA=$(cat "$INPUT_FILE")
    echo "Intermediate data:"
    echo "$INTERMEDIATE_DATA"
else
    echo "ERROR: Intermediate file not found: $INPUT_FILE"
    exit 1
fi

# Process and write final output
OUTPUT_DIR="${OUT_DIR}/chain_final_output"
mkdir -p "$OUTPUT_DIR"

echo "Writing final output to: $OUTPUT_DIR"
echo "Chain complete!" > "$OUTPUT_DIR/result.txt"
echo "Step2 received: ${INTERMEDIATE_DATA}" >> "$OUTPUT_DIR/result.txt"
echo "Final timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$OUTPUT_DIR/result.txt"

echo "=== Chain Step 2 Complete ==="
