#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT=$(cd -- "$SCRIPT_DIR/../../.." && pwd)
PYTHON_BIN=${PYTHON_BIN:-python}
PIPELINE_SCRIPT="$SCRIPT_DIR/test_pipeline.py"
LOG_DIR="$SCRIPT_DIR/log"
PID_FILE="$LOG_DIR/test_pipeline.pid"
NOHUP_LOG="$LOG_DIR/test_pipeline.nohup.out"

mkdir -p "$LOG_DIR"

if [[ -f "$PID_FILE" ]]; then
    existing_pid=$(cat "$PID_FILE")
    if [[ -n "$existing_pid" ]] && kill -0 "$existing_pid" 2>/dev/null; then
        echo "test_pipeline is already running with PID $existing_pid"
        echo "PID file: $PID_FILE"
        exit 1
    fi
    rm -f "$PID_FILE"
fi

cd "$PROJECT_ROOT"

nohup "$PYTHON_BIN" "$PIPELINE_SCRIPT" "$@" >> "$NOHUP_LOG" 2>&1 &
pipeline_pid=$!
echo "$pipeline_pid" > "$PID_FILE"

echo "Started test_pipeline in background"
echo "PID: $pipeline_pid"
echo "PID file: $PID_FILE"
echo "Nohup log: $NOHUP_LOG"
echo "Pipeline log: $LOG_DIR/test_pipeline.log"