#!/bin/sh
# Historical script name; the single-machine product now uses Journal V2.
set -eu
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
exec sh "$ROOT/scripts/build-journal.sh" "$@"
