#!/bin/bash
# LinearJC Job Script: hello.followup
#
# Environment variables provided by executor:
# - LINEARJC_JOB_ID: hello.followup
# - LINEARJC_EXECUTION_ID: hello.followup-YYYYMMDD-HHMMSS
# - LINEARJC_INPUT_DIR: /tmp/linearjc/exec-XXX/inputs
# - LINEARJC_OUTPUT_DIR: /tmp/linearjc/exec-XXX/outputs

set -e

echo "=== Hello Followup Job ==="
echo "Job ID: $LINEARJC_JOB_ID"
echo "Execution ID: $LINEARJC_EXECUTION_ID"
echo ""

# Check inputs (from hello.world output)
echo "Input directory: $LINEARJC_INPUT_DIR"
echo "Input files:"
ls -lah "$LINEARJC_INPUT_DIR/previous_output/" || echo "  (No input files)"
echo ""

# Read input from previous job (find it in extracted archive)
PREV_OUTPUT=$(find "$LINEARJC_INPUT_DIR/previous_output" -type f -name "*.txt" 2>/dev/null | head -1)
if [ -n "$PREV_OUTPUT" ]; then
    echo "Previous job output from: $PREV_OUTPUT"
    cat "$PREV_OUTPUT"
    echo ""
fi

# Do some work
echo "Processing followup..."
sleep 1

# Create output
echo "Creating followup output..."
mkdir -p "$LINEARJC_OUTPUT_DIR/final_output"
echo "=== Followup Job Output ===" > "$LINEARJC_OUTPUT_DIR/final_output/followup_output.txt"
echo "Job execution ID: $LINEARJC_EXECUTION_ID" >> "$LINEARJC_OUTPUT_DIR/final_output/followup_output.txt"
echo "Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$LINEARJC_OUTPUT_DIR/final_output/followup_output.txt"
echo "" >> "$LINEARJC_OUTPUT_DIR/final_output/followup_output.txt"

# Include previous job output
if [ -n "$PREV_OUTPUT" ]; then
    echo "Previous job said:" >> "$LINEARJC_OUTPUT_DIR/final_output/followup_output.txt"
    cat "$PREV_OUTPUT" >> "$LINEARJC_OUTPUT_DIR/final_output/followup_output.txt"
fi

echo "" >> "$LINEARJC_OUTPUT_DIR/final_output/followup_output.txt"
echo "Followup processing complete!" >> "$LINEARJC_OUTPUT_DIR/final_output/followup_output.txt"

echo ""
echo "Output files created:"
ls -lah "$LINEARJC_OUTPUT_DIR/final_output/"
echo ""
echo "=== Followup Job Complete ==="

exit 0
