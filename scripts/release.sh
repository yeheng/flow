#!/bin/sh
# release 构建/测试包装：在受 Xcode 21 ld LINKEDIT 未对齐 bug 影响的机器上
# 自动切换到 rust-lld（见 scripts/macos-lld-linker.sh 头部说明）。
#
# 用法：scripts/release.sh cargo test --release -p flow-backend --test journal_ipc
#       scripts/release.sh cargo build --release --workspace
set -eu

if [ "$#" -eq 0 ]; then
    echo "usage: $0 <cargo command...>" >&2
    exit 64
fi

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export RUSTFLAGS="-Clinker=$ROOT/scripts/macos-lld-linker.sh -Clinker-flavor=ld ${RUSTFLAGS:-}"
exec "$@"
