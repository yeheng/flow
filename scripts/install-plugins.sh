#!/bin/sh
# Install complete bundled plugins; existing installations are left untouched.
set -eu
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROFILE="${1:-debug}"
PLUGIN_ROOT="${FLOW_PLUGINS_DIR:-${FLOW_HOME:-$HOME/.flow}/plugins}"
for spec in "harness-agent:harness:flow-agent-plugin" "llm:llm:flow-llm-plugin" "playwright:playwright:flow-playwright-plugin"; do
  id=${spec%%:*}
  rest=${spec#*:}
  source=${rest%%:*}
  binary=${rest#*:}
  test -f "$ROOT/target/$PROFILE/$binary" || { echo "Missing $binary; build the plugins from the Flow root: cargo build -p flow-harness-plugin -p flow-llm-plugin -p flow-playwright-plugin (add --release for release)" >&2; exit 1; }
  test ! -e "$PLUGIN_ROOT/$id" || { echo "Already installed: $PLUGIN_ROOT/$id; use plugin management to replace it" >&2; exit 1; }
done
mkdir -p "$PLUGIN_ROOT"
for spec in "harness-agent:harness:flow-agent-plugin" "llm:llm:flow-llm-plugin" "playwright:playwright:flow-playwright-plugin"; do
  id=${spec%%:*}
  rest=${spec#*:}
  source=${rest%%:*}
  binary=${rest#*:}
  stage=$(mktemp -d "$PLUGIN_ROOT/.install-$id.XXXXXX")
  mkdir -p "$stage/bin"
  cp "$ROOT/plugins/$source/plugin/plugin.toml" "$stage/plugin.toml"
  cp "$ROOT/target/$PROFILE/$binary" "$stage/bin/$binary"
  if [ "$id" = playwright ]; then
    # playwright 插件多一个 Node 执行环境（wrapper.js + package.json；
    # 依赖不入库也不入内容哈希，装进已部署目录后再 npm install）。
    mkdir -p "$stage/runtime"
    cp "$ROOT/plugins/playwright/runtime/wrapper.js" "$ROOT/plugins/playwright/runtime/package.json" "$stage/runtime/"
  fi
  mv "$stage" "$PLUGIN_ROOT/$id"
  echo "Installed: $PLUGIN_ROOT/$id"
done
if [ -d "$PLUGIN_ROOT/playwright/runtime" ] && [ ! -d "$PLUGIN_ROOT/playwright/runtime/node_modules/playwright" ]; then
  echo "Playwright runtime dependencies missing. To finish setup:"
  echo "  cd $PLUGIN_ROOT/playwright/runtime && npm install && npx playwright install chromium"
fi
