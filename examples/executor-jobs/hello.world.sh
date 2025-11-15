#!/bin/bash
# LinearJC Job Script: hello.world
#
# Environment variables provided by executor:
# - LINEARJC_JOB_ID: hello.world
# - LINEARJC_EXECUTION_ID: hello.world-YYYYMMDD-HHMMSS
# - LINEARJC_INPUT_DIR: /tmp/linearjc/exec-XXX/inputs
# - LINEARJC_OUTPUT_DIR: /tmp/linearjc/exec-XXX/outputs

set -e

echo "=== Hello World Job ==="
echo "Job ID: $LINEARJC_JOB_ID"
echo "Execution ID: $LINEARJC_EXECUTION_ID"
echo ""

# Check inputs
echo "Input directory: $LINEARJC_INPUT_DIR"
echo "Input files:"
ls -lah "$LINEARJC_INPUT_DIR/input_file/" || echo "  (No input files)"
echo ""

# Read input file if it exists (find it in extracted archive)
INPUT_FILE=$(find "$LINEARJC_INPUT_DIR/input_file" -type f -name "*.txt" 2>/dev/null | head -1)
if [ -n "$INPUT_FILE" ]; then
    echo "Input content from: $INPUT_FILE"
    cat "$INPUT_FILE"
    echo ""
fi

# Do some work
echo "Processing..."
sleep 1

# Create output
echo "Creating output..."
mkdir -p "$LINEARJC_OUTPUT_DIR/output_file"
echo "Hello from LinearJC!" > "$LINEARJC_OUTPUT_DIR/output_file/hello_output.txt"
echo "Job execution ID: $LINEARJC_EXECUTION_ID" >> "$LINEARJC_OUTPUT_DIR/output_file/hello_output.txt"
echo "Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$LINEARJC_OUTPUT_DIR/output_file/hello_output.txt"

# Copy input to output (for followup job)
if [ -n "$INPUT_FILE" ]; then
    echo "" >> "$LINEARJC_OUTPUT_DIR/output_file/hello_output.txt"
    echo "Original input:" >> "$LINEARJC_OUTPUT_DIR/output_file/hello_output.txt"
    cat "$INPUT_FILE" >> "$LINEARJC_OUTPUT_DIR/output_file/hello_output.txt"
fi

echo ""
echo "Output files created:"
ls -lah "$LINEARJC_OUTPUT_DIR/output_file/"
echo ""
echo "=== Job Complete ==="

exit 0
