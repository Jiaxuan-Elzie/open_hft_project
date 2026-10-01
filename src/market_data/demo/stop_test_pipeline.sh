#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
LOG_DIR="$SCRIPT_DIR/log"
PID_FILE="$LOG_DIR/test_pipeline.pid"

if [[ ! -f "$PID_FILE" ]]; then
    echo "No PID file found: $PID_FILE"
    exit 1
fi

pipeline_pid=$(cat "$PID_FILE")

if [[ -z "$pipeline_pid" ]]; then
    echo "PID file is empty: $PID_FILE"
    rm -f "$PID_FILE"
    exit 1
fi

if ! kill -0 "$pipeline_pid" 2>/dev/null; then
    echo "Process $pipeline_pid is not running; removing stale PID file"
    rm -f "$PID_FILE"
    exit 0
fi

kill "$pipeline_pid"
rm -f "$PID_FILE"

echo "Stopped test_pipeline"
echo "PID: $pipeline_pid"