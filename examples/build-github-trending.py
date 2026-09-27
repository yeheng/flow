#!/usr/bin/env python3
"""生成 examples/github-trending.workflow.json（引擎的 Definition 结构）。

改脚本 → 重新生成：

    python3 examples/build-github-trending.py

手改 .json 里的 JS 字符串是自找麻烦，改这里。
"""
import json
import pathlib

CODE = r"""
// 把 GitHub Trending 的 HTML 解析成结构化数组。
// 只用正则：script 节点是 rquickjs 沙箱，没有 DOM、没有网络、没有 fs。
function toNum(s) {
  if (!s) return null;
  var n = parseInt(String(s).replace(/,/g, ""), 10);
  return isNaN(n) ? null : n;
}

function decode(s) {
  return s
    .replace(/<[^>]+>/g, "")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&nbsp;/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function parseTrending(html) {
  if (typeof html !== "string") return [];
  var blocks = html.match(/<article class="Box-row[^"]*">[\s\S]*?<\/article>/g) || [];
  var projects = [];
  for (var i = 0; i < blocks.length; i++) {
    var b = blocks[i];
    var repo = b.match(/<h2 class="h3 lh-condensed">\s*<a[^>]*href="\/([^"]+)"/);
    if (!repo) continue;
    var full = repo[1].replace(/^\//, "");
    var parts = full.split("/");
    if (parts.length < 2) continue;
    var desc = b.match(/<p class="col-9[^"]*"[^>]*>\s*([\s\S]*?)\s*<\/p>/);
    var lang = b.match(/itemprop="programmingLanguage">([^<]+)</);
    var today = b.match(/([\d,]+)\s+stars today/);
    var stars = b.match(/href="\/[^"]+\/stargazers"[^>]*>\s*<svg[\s\S]*?<\/svg>\s*([\d,]+)/);
    var forks = b.match(/href="\/[^"]+\/forks"[^>]*>\s*<svg[\s\S]*?<\/svg>\s*([\d,]+)/);
    projects.push({
      rank: projects.length + 1,
      repo: full,
      owner: parts[0],
      name: parts.slice(1).join("/"),
      url: "https://github.com/" + full,
      description: desc ? decode(desc[1]) : "",
      language: lang ? lang[1].trim() : null,
      stars_total: toNum(stars && stars[1]),
      stars_today: toNum(today && today[1]),
      forks: toNum(forks && forks[1])
    });
  }
  return projects;
}

var all = parseTrending(nodes.fetch_all && nodes.fetch_all.body);
var zh = parseTrending(nodes.fetch_zh && nodes.fetch_zh.body);

// 页面结构变了要炸出来，不要静悄悄地写一份空 JSON
if (all.length === 0) {
  throw new Error("全量榜解析出 0 个项目：GitHub Trending 页面结构可能已变");
}

// 非中文榜 = 全量榜里排除掉「口语=中文」榜已出现的仓库
var zhSet = {};
for (var k = 0; k < zh.length; k++) zhSet[zh[k].repo.toLowerCase()] = true;
var nonZh = [];
for (var j = 0; j < all.length; j++) {
  if (zhSet[all[j].repo.toLowerCase()]) continue;
  var p = all[j];
  nonZh.push({
    rank: nonZh.length + 1,
    repo: p.repo,
    owner: p.owner,
    name: p.name,
    url: p.url,
    description: p.description,
    language: p.language,
    stars_total: p.stars_total,
    stars_today: p.stars_today,
    forks: p.forks
  });
}

return {
  generated_at: new Date().toISOString(),
  since: "daily",
  sources: {
    all: "https://github.com/trending?since=daily",
    chinese: "https://github.com/trending?since=daily&spoken_language_code=zh"
  },
  counts: { chinese: zh.length, non_chinese: nonZh.length, all: all.length },
  chinese: zh,
  non_chinese: nonZh
};
""".strip()

HEADERS = {"User-Agent": "flow-github-trending", "Accept": "text/html"}


def http(node_id, name, url, x, y):
    return {
        "id": node_id,
        "type": "http_call",
        "name": name,
        "position": {"x": x, "y": y},
        "params": {
            "method": "GET",
            "url": url,
            "headers": HEADERS,
            "timeout_ms": 30000,
        },
    }


definition = {
    "nodes": [
        {"id": "start", "type": "start", "name": "开始", "position": {"x": 40, "y": 200}},
        http(
            "fetch_all",
            "Trending 全量榜",
            "https://github.com/trending?since=daily",
            280,
            80,
        ),
        http(
            "fetch_zh",
            "Trending 中文榜",
            "https://github.com/trending?since=daily&spoken_language_code=zh",
            280,
            320,
        ),
        {
            "id": "parse",
            "type": "script",
            "name": "解析并分成中文 / 非中文",
            "position": {"x": 560, "y": 200},
            "params": {"code": CODE, "timeout_ms": 10000},
        },
        {"id": "done", "type": "end", "name": "结束", "position": {"x": 840, "y": 200}},
    ],
    "edges": [
        {"from": "start", "to": "fetch_all"},
        {"from": "start", "to": "fetch_zh"},
        {"from": "fetch_all", "to": "parse"},
        {"from": "fetch_zh", "to": "parse"},
        {"from": "parse", "to": "done"},
    ],
}

doc = {
    "name": "github-trending",
    "description": "拉取 GitHub Trending 日报（全量 + spoken_language_code=zh），输出 中文 / 非中文 两组项目 JSON",
    "definition": definition,
}

out = pathlib.Path(__file__).with_name("github-trending.workflow.json")
out.write_text(json.dumps(doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print("wrote", out, len(out.read_text(encoding="utf-8")), "bytes")
