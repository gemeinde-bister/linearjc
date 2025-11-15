#!/bin/bash
# Job: hello.long
# Long-running job for testing timeouts

set -e

echo "=== Long-Running Job Started ==="
echo "Job ID: $LINEARJC_JOB_ID"
echo "Execution ID: $LINEARJC_EXECUTION_ID"
echo "This job will sleep for 60 seconds..."
echo

# Sleep for 60 seconds (long enough to kill mid-execution)
for i in {1..60}; do
    echo "  Sleeping... $i/60"
    sleep 1
done

# Write output
mkdir -p "$LINEARJC_OUTPUT_DIR/output_file"
cat > "$LINEARJC_OUTPUT_DIR/output_file/result.txt" <<EOF
Long job completed!
Execution ID: $LINEARJC_EXECUTION_ID
Timestamp: $(date -u +"%Y-%m-%dT%H:%M:%SZ")
EOF

echo "=== Long-Running Job Completed ==="
exit 0
