#!/usr/bin/env bash
set -euo pipefail

SCRIPT="${1:-examples/test.rhai}"
PORT=1602
DATA_DIR="${TMPDIR:-/tmp}/amp-solo-data"
PROVIDERS_DIR="${TMPDIR:-/tmp}/amp-solo-providers"
CONFIG_FILE="${TMPDIR:-/tmp}/amp_solo.toml"

# Write a minimal config if one doesn't already exist
if [ ! -f "$CONFIG_FILE" ]; then
    mkdir -p "$DATA_DIR" "$PROVIDERS_DIR"
    printf 'data_dir = "%s"\nproviders_dir = "%s"\n' "$DATA_DIR" "$PROVIDERS_DIR" > "$CONFIG_FILE"
fi

echo "Starting ampd..."
AMP_CONFIG="$CONFIG_FILE" ampd solo --flight-server &
AMPD_PID=$!
trap 'kill "$AMPD_PID" 2>/dev/null; wait "$AMPD_PID" 2>/dev/null' EXIT

echo "Waiting for grpc://localhost:$PORT..."
for i in $(seq 1 30); do
    if nc -z localhost "$PORT" 2>/dev/null; then
        echo "ampd ready."
        break
    fi
    sleep 0.5
    if [ "$i" -eq 30 ]; then
        echo "ampd did not start in time" >&2
        exit 1
    fi
done

cargo run --example rhai_runner "$SCRIPT"
