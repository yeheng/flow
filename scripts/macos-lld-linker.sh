#!/bin/sh
# macOS lld 链接器 shim（修复 Xcode 21 ld-27037.1 的 LINKEDIT 未对齐 bug）。
#
# 问题：本机 Xcode 21 的新链接器对部分二进制（大符号表的 proc-macro dylib，
# 如 sqlx-macros）把 LC_SYMTAB.stroff 放在 4 mod 8 偏移；系统 dyld 要求
# 8 字节对齐 → dlopen "mis-aligned LINKEDIT string pool" → cargo 无法加载
# proc-macro，release 构建确定性失败（debug 布局偶然对齐故不受影响）。
#
# 修复：改用 rustc 自带的 rust-lld（以 ld64.lld 身份），并把 27.0 SDK 的
# syslibroot 重写到 26.5 SDK（rust-lld 不识别 27.0 TAPI 的 arm64e.x1 架构
# token；26.5 没有）。
#
# 用法：经 scripts/release.sh 自动启用；或
#   RUSTFLAGS="-Clinker=$PWD/scripts/macos-lld-linker.sh -Clinker-flavor=ld"
set -eu

SYSROOT="$(rustc --print sysroot)"
LULD="$SYSROOT/lib/rustlib/aarch64-apple-darwin/bin/rust-lld"
[ -x "$LULD" ] || LULD="$SYSROOT/lib/rustlib/x86_64-apple-darwin/bin/rust-lld"

OLD_SDK="/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX27.0.sdk"
NEW_SDK=/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk
if [ -d "$NEW_SDK" ]; then
    n=$#
    while [ "$n" -gt 0 ]; do
        arg=$1
        shift
        case "$arg" in
            "$OLD_SDK"|*"/MacOSX27.0.sdk")
                arg=$NEW_SDK
                ;;
        esac
        set -- "$@" "$arg"
        n=$((n - 1))
    done
fi

exec "$LULD" -flavor darwin "$@"
