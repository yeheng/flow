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

# ---- Harness agent 策展节点：总结榜单趋势 + 推荐有趣项目 ----
# 内置 flow-harness-agent（conga-host 无头装配，FullAuto：可读写 workdir、联网、起 shell）。
# 与 llm 节点的区别：
#   - 模型配置（base_url/model/api）在 flow.toml 的 [harness.agent]，不在这里；
#   - api_key 只存密钥**名称**，真值经密钥体系（stored 优先、FLOW_SECRET_* 兜底）注入，
#     任何环境变量 / journal / 日志都不携带；
#   - workdir 是 agent 工具沙箱根，必须是已存在的绝对路径（运行前 canonicalize 校验）。
AGENT_SECRET = "DEEPSEEK_API_KEY"            # 密钥名 → FLOW_SECRET_DEEPSEEK_API_KEY 或 Web 密钥管理
AGENT_WORKDIR = "/absolute/path/to/workdir"  # TODO: 改成已存在的绝对路径（agent 沙箱根）

# prompt 经 stdin 进 runner；${nodes.parse} 整值插值成 parse 节点的 JSON。
AGENT_PROMPT = """你是一位资深的技术编辑与开源项目策展人，擅长从海量项目里挑出真正值得关注的那几个，并用简洁有力的中文讲清楚它是什么、解决什么问题、妙在哪里。判断独立犀利，不写空话套话。这是纯分析任务，直接给出最终结果即可，无需读写文件或执行命令。

以下是今天 GitHub Trending 的榜单数据（生成于 ${nodes.parse.generated_at}），含中文榜与非中文榜两组项目（JSON）：

${nodes.parse}

请作为策展人完成以下任务，严格只输出一个 JSON 对象（不要任何解释文字，不要用 markdown 代码块包裹）。JSON 结构如下：

{
  "overview": "<2-3 句话概括今天榜单的整体趋势：哪些技术方向在升温、有什么共性>",
  "picks": [
    {
      "repo": "<owner/name，原样保留>",
      "url": "<仓库地址>",
      "language": "<主要语言，未知填 null>",
      "stars_today": <今日新增 star 数，未知填 null>,
      "intro": "<50-120 字中文介绍：这是什么项目、解决什么痛点、为什么现在值得看>",
      "highlights": ["<卖点1>", "<卖点2>", "<卖点3>"]
    }
  ]
}

挑选标准：从两组榜单里挑 5-8 个最有趣或最值得关注的项目，按有趣程度从高到低排序；优先新颖独特、解决真实痛点、增长势头猛或技术上有巧思的项目；避开纯教程、纯资源列表、营销性质的项目；尽量覆盖不同技术方向与语言。highlights 给 3-5 条，每条一句话，突出最独特、最实用或最有前景的地方。"""

DIGEST_CODE = r"""
// harness agent 输出 {exit_code, stdout, stderr, agent, result?}。
// result 是 stdout 以 { / [ 开头且能解析成对象/数组时的投影；缺省回退 stdout 手工解析。
var out = nodes.recommend || {};
var parsed = out.result;
if (parsed === undefined || parsed === null) {
  var text = typeof out.stdout === "string" ? out.stdout : "";
  // 有些模型会用 ```json ... ``` 包裹，先剥掉再解析
  text = text.trim().replace(/^```[a-zA-Z]*\s*/, "").replace(/\s*```$/, "");
  if (text === "") {
    throw new Error("harness agent 无文本输出：" + JSON.stringify(out).slice(0, 800));
  }
  try {
    parsed = JSON.parse(text);
  } catch (e) {
    throw new Error("agent 返回的不是合法 JSON：" + e.message + "；原文开头：" + text.slice(0, 800));
  }
}
return {
  generated_at: new Date().toISOString(),
  model: (out.agent && out.agent.model) || null,
  overview: typeof parsed.overview === "string" ? parsed.overview : "",
  picks: Array.isArray(parsed.picks) ? parsed.picks : []
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
    "semantics": 2,
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
        {
            "id": "recommend",
            "type": "harness",
            "name": "Harness agent 总结并推荐有趣项目",
            "position": {"x": 820, "y": 200},
            "params": {
                "mode": "agent",
                "prompt": AGENT_PROMPT,
                "workdir": AGENT_WORKDIR,
                "api_key": AGENT_SECRET,
                "timeout_ms": 300000,
            },
        },
        {
            "id": "digest",
            "type": "script",
            "name": "整理推荐结果",
            "position": {"x": 1080, "y": 200},
            "params": {"code": DIGEST_CODE, "timeout_ms": 10000},
        },
        {"id": "done", "type": "end", "name": "结束", "position": {"x": 1340, "y": 200}},
    ],
    "edges": [
        {"from": "start", "to": "fetch_all"},
        {"from": "start", "to": "fetch_zh"},
        {"from": "fetch_all", "to": "parse"},
        {"from": "fetch_zh", "to": "parse"},
        {"from": "parse", "to": "recommend"},
        {"from": "recommend", "to": "digest"},
        {"from": "digest", "to": "done"},
    ],
}

doc = {
    "name": "github-trending",
    "description": "拉取 GitHub Trending 日报（全量 + spoken_language_code=zh），解析成 中文 / 非中文 两组项目后，用内置 harness agent 总结榜单趋势并推荐有趣的项目（含介绍与卖点）。需配置 [harness.agent] 模型、密钥 DEEPSEEK_API_KEY，并把 recommend 节点的 workdir 改成已存在的绝对路径",
    "definition": definition,
}

out = pathlib.Path(__file__).with_name("github-trending.workflow.json")
out.write_text(json.dumps(doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print("wrote", out, len(out.read_text(encoding="utf-8")), "bytes")
