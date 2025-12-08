#!/bin/sh
# Smoke test script - reads input, transforms, writes output
# Verifies the complete LinearJC flow works
# SPEC.md v0.5.0: Uses LJC_IN, LJC_OUT, LJC_TMP environment variables

set -e

echo "=== Echo Test Job ==="
echo "Working directory: $(pwd)"
echo "LJC_IN: ${LJC_IN:-not set}"
echo "LJC_OUT: ${LJC_OUT:-not set}"
echo "LJC_TMP: ${LJC_TMP:-not set}"
echo "LJC_JOB_ID: ${LJC_JOB_ID:-not set}"
echo "LJC_EXECUTION_ID: ${LJC_EXECUTION_ID:-not set}"

# Use short-form env vars (SPEC.md v0.5.0)
IN_DIR="${LJC_IN:-$LINEARJC_IN_DIR}"
OUT_DIR="${LJC_OUT:-$LINEARJC_OUT_DIR}"

# Read input (kind: dir means input is a directory)
INPUT_FILE="${IN_DIR}/echo_input/input.txt"
if [ ! -f "$INPUT_FILE" ]; then
    echo "ERROR: Input file not found: $INPUT_FILE"
    echo "Contents of IN_DIR:"
    ls -la "${IN_DIR}/" 2>/dev/null || echo "IN_DIR directory does not exist"
    if [ -d "${IN_DIR}/echo_input" ]; then
        echo "Contents of echo_input:"
        ls -la "${IN_DIR}/echo_input/" 2>/dev/null
    fi
    exit 1
fi

echo "Input content:"
cat "$INPUT_FILE"

# Create output directory (kind: dir means we write to a directory)
OUTPUT_DIR="${OUT_DIR}/echo_output"
mkdir -p "$OUTPUT_DIR"

# Transform: copy input and add timestamp
cp "$INPUT_FILE" "$OUTPUT_DIR/output.txt"
echo "" >> "$OUTPUT_DIR/output.txt"
echo "Processed by echo.test at $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$OUTPUT_DIR/output.txt"

echo ""
echo "Output content:"
cat "$OUTPUT_DIR/output.txt"

echo ""
echo "=== Echo Test Complete ==="
