#!/bin/sh
# Build the standalone server and the sibling binaries loaded by its executor.
set -eu
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
cargo build --locked -p flow-rpc --bin flow-journal-server \
  -p flow-plugin-runtime --bin flow-executor \
  -p flow-harness-plugin --bin flow-agent-plugin \
  -p flow-llm-plugin --bin flow-llm-plugin "$@"
echo "Server and execution plugins ready in target/debug (or the selected Cargo profile)."
