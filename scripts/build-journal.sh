#!/bin/sh
# Build and package the V2 Journal product and its execution tools.
set -eu
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST="$ROOT/dist/journal"
cd "$ROOT"
BINS="flow-journal-server flow-executor flow-agent flow-cli flow-journal-tool flow-journal-bench flow-journal-dev"
mkdir -p "$DIST/bin"
"$ROOT/scripts/release.sh" cargo build --release --workspace --locked "$@"
for bin in $BINS; do
  cp "$ROOT/target/release/$bin" "$DIST/bin/$bin"
done
cat > "$DIST/run-server.sh" <<'EOF'
#!/bin/sh
set -eu
DIR="$(cd "$(dirname "$0")" && pwd)"
: "${FLOW_JOURNAL_TOKEN:?FLOW_JOURNAL_TOKEN is required (at least 32 bytes)}"
export FLOW_DATA_DIR="${FLOW_DATA_DIR:-$DIR/secrets}"
export FLOW_JOURNAL_DATA_DIR="${FLOW_JOURNAL_DATA_DIR:-$DIR/journal}"
export FLOW_JOURNAL_ADDR="${FLOW_JOURNAL_ADDR:-127.0.0.1:9802}"
export FLOW_JOURNAL_HTTP_ADDR="${FLOW_JOURNAL_HTTP_ADDR:-127.0.0.1:9803}"
exec "$DIR/bin/flow-journal-server" "$@"
EOF
chmod +x "$DIST/run-server.sh"
echo "built: $DIST/bin/ ($BINS)"
echo "entry: $DIST/run-server.sh"
