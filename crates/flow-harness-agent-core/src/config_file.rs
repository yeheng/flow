//! `config.toml`: file-based base config layered under the environment.
//!
//! Layering, lowest → highest:
//! 1. `config.toml` (this module) — `./.conga/config.toml` or `~/.conga/config.toml`
//! 2. `.env` (dotenvy — loaded here FIRST, so it wins over the file)
//! 3. real process env (always wins: we only `set_var` what is unset)
//! 4. `~/.conga/settings.json` (web UI, re-read every turn — separate layer)
//!
//! Discovery: `CONGA_CONFIG` (fail-loud when the file is missing) →
//! `./.conga/config.toml` → `~/.conga/config.toml`; first existing file wins.
//! No file anywhere → no-op (env-only, backward compatible).
//!
//! Sections mirror the `CONGA_*` env vars documented in `.env.example`;
//! see `config.example.toml` for a full annotated template. Unknown keys
//! are a parse error (typo protection). Empty string values are treated
//! as "unset" so templates can ship placeholder blanks.

use std::path::{Path, PathBuf};

use serde::Deserialize;

/// Errors from loading `config.toml`.
#[derive(Debug, thiserror::Error)]
pub enum ConfigFileError {
    #[error("CONGA_CONFIG is set but the file does not exist: {0}")]
    ExplicitMissing(PathBuf),
    #[error("cannot read config file {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid config.toml {path} (unknown key or bad value): {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
}

/// The whole `config.toml`. Every section is optional; every key optional.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConfigFile {
    pub llm: Option<LlmSection>,
    pub fast_llm: Option<LlmSection>,
    pub proxy: Option<ProxySection>,
    pub tunables: Option<TunablesSection>,
    pub gateway: Option<GatewaySection>,
    pub compact: Option<CompactSection>,
    pub tools: Option<ToolsSection>,
    pub search: Option<SearchSection>,
    pub rag: Option<RagSection>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LlmSection {
    pub base_url: Option<String>,
    pub key: Option<String>,
    pub model: Option<String>,
    /// `openai` (default) or `anthropic`.
    pub api: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProxySection {
    /// LLM egress proxy for both http and https (`CONGA_LLM_PROXY`).
    pub llm: Option<String>,
    pub llm_http: Option<String>,
    pub llm_https: Option<String>,
    /// Tool traffic proxy for fetch / web_search (`CONGA_TOOL_PROXY`).
    pub tool: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TunablesSection {
    pub max_turns: Option<u64>,
    pub max_tool_calls: Option<u64>,
    pub max_tokens: Option<u64>,
    pub retry_max: Option<u64>,
    pub retry_initial_ms: Option<u64>,
    pub retry_max_ms: Option<u64>,
    pub tool_timeout_s: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GatewaySection {
    pub port: Option<u64>,
    pub host: Option<String>,
    /// suggest | auto-edit | full-auto | plan.
    pub mode: Option<String>,
    pub approval_timeout_s: Option<u64>,
    pub token: Option<String>,
    /// Comma-separated extra CORS origins.
    pub cors_origins: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CompactSection {
    pub context_window: Option<u64>,
    pub threshold_pct: Option<u64>,
    pub target_pct: Option<u64>,
    pub max_messages: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ToolsSection {
    /// Comma-separated external command whitelist.
    pub external_tools: Option<String>,
    /// MCP config file path (default `~/.conga/mcp.json`).
    pub mcp_config: Option<String>,
    pub mcp_call_timeout_s: Option<u64>,
    /// Disable the fetch tool's SSRF guard (trusted LANs only).
    pub fetch_allow_private_net: Option<bool>,
    /// Run bash tool commands in a filesystem sandbox.
    pub sandbox: Option<bool>,
    /// Project dir: tool sandbox root + project skills base.
    pub project_dir: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SearchSection {
    pub provider: Option<String>,
    pub brave_api_key: Option<String>,
    pub tavily_api_key: Option<String>,
    pub serper_api_key: Option<String>,
    pub serpapi_api_key: Option<String>,
    pub exa_api_key: Option<String>,
    pub firecrawl_api_key: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RagSection {
    /// conga-rag config file (overrides `./rag.toml` discovery).
    pub config: Option<String>,
    pub embed_base_url: Option<String>,
    pub embed_key: Option<String>,
    pub embed_model: Option<String>,
    pub embed_batch: Option<u64>,
    pub store_path: Option<String>,
}

impl ConfigFile {
    /// Flatten to `(env var name, value)` pairs, skipping unset/empty keys.
    pub fn env_pairs(&self) -> Vec<(String, String)> {
        let mut out = Vec::new();
        if let Some(s) = &self.llm {
            s.push(&mut out, "CONGA_LLM");
        }
        if let Some(s) = &self.fast_llm {
            s.push(&mut out, "CONGA_FAST_LLM");
        }
        if let Some(s) = &self.proxy {
            s.push(&mut out);
        }
        if let Some(s) = &self.tunables {
            s.push(&mut out);
        }
        if let Some(s) = &self.gateway {
            s.push(&mut out);
        }
        if let Some(s) = &self.compact {
            s.push(&mut out);
        }
        if let Some(s) = &self.tools {
            s.push(&mut out);
        }
        if let Some(s) = &self.search {
            s.push(&mut out);
        }
        if let Some(s) = &self.rag {
            s.push(&mut out);
        }
        out
    }
}

/// String env values with an env-name prefix (`CONGA_LLM` + `_BASE_URL`).
impl LlmSection {
    fn push(&self, out: &mut Vec<(String, String)>, prefix: &str) {
        push_str(out, &format!("{prefix}_BASE_URL"), &self.base_url);
        push_str(out, &format!("{prefix}_KEY"), &self.key);
        push_str(out, &format!("{prefix}_MODEL"), &self.model);
        push_str(out, &format!("{prefix}_API"), &self.api);
    }
}

impl ProxySection {
    fn push(&self, out: &mut Vec<(String, String)>) {
        push_str(out, "CONGA_LLM_PROXY", &self.llm);
        push_str(out, "CONGA_LLM_HTTP_PROXY", &self.llm_http);
        push_str(out, "CONGA_LLM_HTTPS_PROXY", &self.llm_https);
        push_str(out, "CONGA_TOOL_PROXY", &self.tool);
    }
}

impl TunablesSection {
    fn push(&self, out: &mut Vec<(String, String)>) {
        push_num(out, "CONGA_MAX_TURNS", &self.max_turns);
        push_num(out, "CONGA_MAX_TOOL_CALLS", &self.max_tool_calls);
        push_num(out, "CONGA_MAX_TOKENS", &self.max_tokens);
        push_num(out, "CONGA_RETRY_MAX", &self.retry_max);
        push_num(out, "CONGA_RETRY_INITIAL_MS", &self.retry_initial_ms);
        push_num(out, "CONGA_RETRY_MAX_MS", &self.retry_max_ms);
        push_num(out, "CONGA_TOOL_TIMEOUT_S", &self.tool_timeout_s);
    }
}

impl GatewaySection {
    fn push(&self, out: &mut Vec<(String, String)>) {
        push_num(out, "CONGA_GATEWAY_PORT", &self.port);
        push_str(out, "CONGA_GATEWAY_HOST", &self.host);
        push_str(out, "CONGA_GATEWAY_MODE", &self.mode);
        push_num(out, "CONGA_APPROVAL_TIMEOUT_S", &self.approval_timeout_s);
        push_str(out, "CONGA_GATEWAY_TOKEN", &self.token);
        push_str(out, "CONGA_GATEWAY_CORS_ORIGINS", &self.cors_origins);
    }
}

impl CompactSection {
    fn push(&self, out: &mut Vec<(String, String)>) {
        push_num(out, "CONGA_CONTEXT_WINDOW", &self.context_window);
        push_num(out, "CONGA_COMPACT_THRESHOLD_PCT", &self.threshold_pct);
        push_num(out, "CONGA_COMPACT_TARGET_PCT", &self.target_pct);
        push_num(out, "CONGA_COMPACT_MAX_MESSAGES", &self.max_messages);
    }
}

impl ToolsSection {
    fn push(&self, out: &mut Vec<(String, String)>) {
        push_str(out, "CONGA_EXTERNAL_TOOLS", &self.external_tools);
        push_str(out, "CONGA_MCP_CONFIG", &self.mcp_config);
        push_num(out, "CONGA_MCP_CALL_TIMEOUT_S", &self.mcp_call_timeout_s);
        push_bool(
            out,
            "CONGA_FETCH_ALLOW_PRIVATE_NET",
            &self.fetch_allow_private_net,
        );
        push_bool(out, "CONGA_SANDBOX", &self.sandbox);
        push_str(out, "CONGA_PROJECT_DIR", &self.project_dir);
    }
}

impl SearchSection {
    fn push(&self, out: &mut Vec<(String, String)>) {
        push_str(out, "CONGA_SEARCH_PROVIDER", &self.provider);
        push_str(out, "CONGA_BRAVE_API_KEY", &self.brave_api_key);
        push_str(out, "CONGA_TAVILY_API_KEY", &self.tavily_api_key);
        push_str(out, "CONGA_SERPER_API_KEY", &self.serper_api_key);
        push_str(out, "CONGA_SERPAPI_API_KEY", &self.serpapi_api_key);
        push_str(out, "CONGA_EXA_API_KEY", &self.exa_api_key);
        push_str(out, "CONGA_FIRECRAWL_API_KEY", &self.firecrawl_api_key);
    }
}

impl RagSection {
    fn push(&self, out: &mut Vec<(String, String)>) {
        push_str(out, "CONGA_RAG_CONFIG", &self.config);
        push_str(out, "CONGA_RAG_EMBED_BASE_URL", &self.embed_base_url);
        push_str(out, "CONGA_RAG_EMBED_KEY", &self.embed_key);
        push_str(out, "CONGA_RAG_EMBED_MODEL", &self.embed_model);
        push_num(out, "CONGA_RAG_EMBED_BATCH", &self.embed_batch);
        push_str(out, "CONGA_RAG_STORE_PATH", &self.store_path);
    }
}

fn push_str(out: &mut Vec<(String, String)>, name: &str, value: &Option<String>) {
    // Empty string = unset (templates may ship placeholder blanks).
    if let Some(v) = value.as_deref().filter(|v| !v.is_empty()) {
        out.push((name.to_string(), v.to_string()));
    }
}

fn push_num(out: &mut Vec<(String, String)>, name: &str, value: &Option<u64>) {
    if let Some(v) = value {
        out.push((name.to_string(), v.to_string()));
    }
}

/// Bools serialize as "1"/"0" — every parse site checks `== "1"`
/// (fetch also accepts "true", so "1" is the safe common form).
fn push_bool(out: &mut Vec<(String, String)>, name: &str, value: &Option<bool>) {
    if let Some(v) = value {
        out.push((name.to_string(), if *v { "1" } else { "0" }.to_string()));
    }
}

/// Load `.conga/config.toml` and apply it as the base config layer.
///
/// Runs dotenv first (`.env` must win over the file), then sets each config
/// value as an env var ONLY when that var is not already set (env overrides
/// the file). Call this once at the top of `main`, before any `CONGA_*`
/// env read. `Ok(None)` = no config file found (env-only startup).
pub fn apply() -> Result<Option<PathBuf>, ConfigFileError> {
    let _ = dotenvy::dotenv(); // .env fills first → it overrides config.toml
    let lookup = |k: &str| std::env::var(k);
    let path = match discover(
        &lookup,
        Path::new(".conga/config.toml"),
        dirs::home_dir().as_deref(),
    )? {
        Some(p) => p,
        None => return Ok(None),
    };
    let raw = std::fs::read_to_string(&path).map_err(|e| ConfigFileError::Read {
        path: path.clone(),
        source: e,
    })?;
    let cfg: ConfigFile = toml::from_str(&raw).map_err(|e| ConfigFileError::Parse {
        path: path.clone(),
        source: e,
    })?;
    overlay(&cfg.env_pairs(), &lookup, &|k, v| std::env::set_var(k, v));
    Ok(Some(path))
}

/// `CONGA_CONFIG` (fail-loud when missing) → `project` → `home/.conga/config.toml`.
fn discover(
    env: &dyn Fn(&str) -> Result<String, std::env::VarError>,
    project: &Path,
    home: Option<&Path>,
) -> Result<Option<PathBuf>, ConfigFileError> {
    if let Ok(p) = env("CONGA_CONFIG") {
        let p = PathBuf::from(&p);
        if p.exists() {
            return Ok(Some(p));
        }
        return Err(ConfigFileError::ExplicitMissing(p));
    }
    if project.exists() {
        return Ok(Some(project.to_path_buf()));
    }
    Ok(home
        .map(|h| h.join(".conga/config.toml"))
        .filter(|p| p.exists()))
}

/// Set each pair only when the env var is unset — env wins.
fn overlay(
    pairs: &[(String, String)],
    get: &dyn Fn(&str) -> Result<String, std::env::VarError>,
    set: &dyn Fn(&str, &str),
) {
    for (k, v) in pairs {
        if get(k).is_err() {
            set(k, v);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn no_env(_: &str) -> Result<String, std::env::VarError> {
        Err(std::env::VarError::NotPresent)
    }

    fn fake_env(
        pairs: &[(&str, &str)],
    ) -> impl Fn(&str) -> Result<String, std::env::VarError> + use<> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k: &str| map.get(k).cloned().ok_or(std::env::VarError::NotPresent)
    }

    const FULL: &str = r#"
[llm]
base_url = "https://api.x.com/v1"
key = "sk-1"
model = "m1"
api = "anthropic"

[fast_llm]
base_url = "https://fast.x.com/v1"
key = "sk-2"
model = "m2"

[proxy]
llm = "http://p:8080"
tool = "socks5://127.0.0.1:1080"

[tunables]
max_turns = 7
max_tool_calls = 8
max_tokens = 123
retry_max = 3
retry_initial_ms = 100
retry_max_ms = 2000
tool_timeout_s = 30

[gateway]
port = 3000
host = "0.0.0.0"
mode = "plan"
approval_timeout_s = 60
token = "t"
cors_origins = "https://a.example"

[compact]
context_window = 64000
threshold_pct = 75
target_pct = 40
max_messages = 20

[tools]
external_tools = "rg,jq"
mcp_config = "/tmp/mcp.json"
mcp_call_timeout_s = 90
fetch_allow_private_net = true
sandbox = true
project_dir = "/tmp/proj"

[search]
provider = "brave"
brave_api_key = "bk"
tavily_api_key = "tk"
serper_api_key = "sp"
serpapi_api_key = "spp"
exa_api_key = "ex"
firecrawl_api_key = "fc"

[rag]
config = "/tmp/rag.toml"
embed_base_url = "https://e.x.com/v1"
embed_key = "ek"
embed_model = "emb-1"
embed_batch = 8
store_path = "/tmp/index.db"
"#;

    fn names(cfg: &ConfigFile) -> Vec<String> {
        cfg.env_pairs().into_iter().map(|(k, _)| k).collect()
    }

    #[test]
    fn parses_and_maps_every_section() {
        let cfg: ConfigFile = toml::from_str(FULL).unwrap();
        let pairs = cfg.env_pairs();
        let get = |k: &str| {
            pairs
                .iter()
                .find(|(n, _)| n == k)
                .map(|(_, v)| v.clone())
                .unwrap_or_else(|| panic!("missing {k}"))
        };
        assert_eq!(get("CONGA_LLM_BASE_URL"), "https://api.x.com/v1");
        assert_eq!(get("CONGA_LLM_API"), "anthropic");
        assert_eq!(get("CONGA_FAST_LLM_MODEL"), "m2");
        assert_eq!(get("CONGA_LLM_PROXY"), "http://p:8080");
        assert_eq!(get("CONGA_TOOL_PROXY"), "socks5://127.0.0.1:1080");
        assert_eq!(get("CONGA_MAX_TURNS"), "7");
        assert_eq!(get("CONGA_TOOL_TIMEOUT_S"), "30");
        assert_eq!(get("CONGA_GATEWAY_PORT"), "3000");
        assert_eq!(get("CONGA_GATEWAY_MODE"), "plan");
        assert_eq!(get("CONGA_APPROVAL_TIMEOUT_S"), "60");
        assert_eq!(get("CONGA_CONTEXT_WINDOW"), "64000");
        assert_eq!(get("CONGA_COMPACT_MAX_MESSAGES"), "20");
        assert_eq!(get("CONGA_EXTERNAL_TOOLS"), "rg,jq");
        assert_eq!(get("CONGA_FETCH_ALLOW_PRIVATE_NET"), "1");
        assert_eq!(get("CONGA_SANDBOX"), "1");
        assert_eq!(get("CONGA_PROJECT_DIR"), "/tmp/proj");
        assert_eq!(get("CONGA_SEARCH_PROVIDER"), "brave");
        assert_eq!(get("CONGA_FIRECRAWL_API_KEY"), "fc");
        assert_eq!(get("CONGA_RAG_CONFIG"), "/tmp/rag.toml");
        assert_eq!(get("CONGA_RAG_EMBED_BATCH"), "8");
        // Complete coverage: 4+3+2+7+6+4+6+7+6 = 45 pairs (fast_llm omits api).
        assert_eq!(pairs.len(), 45, "all keys mapped: {:?}", names(&cfg));
    }

    #[test]
    fn empty_file_is_noop() {
        let cfg: ConfigFile = toml::from_str("").unwrap();
        assert!(cfg.env_pairs().is_empty());
    }

    #[test]
    fn empty_string_values_are_skipped() {
        let cfg: ConfigFile = toml::from_str(
            r#"
[llm]
base_url = "https://api.x.com/v1"
key = ""
"#,
        )
        .unwrap();
        let pairs = cfg.env_pairs();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, "CONGA_LLM_BASE_URL");
    }

    #[test]
    fn false_bool_serializes_as_zero() {
        let cfg: ConfigFile =
            toml::from_str("[tools]\nsandbox = false\nfetch_allow_private_net = true\n").unwrap();
        let pairs = cfg.env_pairs();
        let get = |k: &str| pairs.iter().find(|(n, _)| n == k).unwrap().1.clone();
        assert_eq!(get("CONGA_SANDBOX"), "0");
        assert_eq!(get("CONGA_FETCH_ALLOW_PRIVATE_NET"), "1");
    }

    #[test]
    fn unknown_key_fails_loud() {
        let e = toml::from_str::<ConfigFile>("[llm]\nbase_urL = \"typo\"\n").unwrap_err();
        assert!(e.to_string().contains("base_urL"), "must name the key: {e}");
        let e = toml::from_str::<ConfigFile>("[not_a_section]\nx = 1\n").unwrap_err();
        assert!(
            e.to_string().contains("not_a_section"),
            "must name the section: {e}"
        );
    }

    #[test]
    fn overlay_env_wins_and_unset_gets_set() {
        let pairs = vec![
            ("CONGA_LLM_MODEL".to_string(), "file-model".to_string()),
            ("CONGA_MAX_TURNS".to_string(), "9".to_string()),
        ];
        let set = std::cell::RefCell::new(Vec::new());
        overlay(
            &pairs,
            &fake_env(&[("CONGA_LLM_MODEL", "env-model")]),
            &|k, v| set.borrow_mut().push((k.to_string(), v.to_string())),
        );
        // Env-set var untouched; unset var applied from the file.
        assert_eq!(
            set.into_inner(),
            vec![("CONGA_MAX_TURNS".to_string(), "9".to_string())]
        );
    }

    #[test]
    fn discover_order_explicit_project_home() {
        let tmp = tempfile::tempdir().unwrap();
        let project = tmp.path().join(".conga/config.toml");
        std::fs::create_dir_all(project.parent().unwrap()).unwrap();
        std::fs::write(&project, "").unwrap();
        let home = tmp.path().join("home");
        std::fs::create_dir_all(home.join(".conga")).unwrap();
        std::fs::write(home.join(".conga/config.toml"), "").unwrap();

        // 1. CONGA_CONFIG wins when the file exists.
        let explicit = tmp.path().join("explicit.toml");
        std::fs::write(&explicit, "").unwrap();
        let found = discover(
            &fake_env(&[("CONGA_CONFIG", explicit.to_str().unwrap())]),
            &project,
            Some(&home),
        )
        .unwrap();
        assert_eq!(found.as_deref(), Some(explicit.as_path()));

        // 2. Project-level beats home.
        let found = discover(&no_env, &project, Some(&home)).unwrap();
        assert_eq!(found.as_deref(), Some(project.as_path()));

        // 3. Home fallback when no project file.
        let found = discover(&no_env, &tmp.path().join("no-such"), Some(&home)).unwrap();
        assert_eq!(
            found.as_deref(),
            Some(home.join(".conga/config.toml").as_path())
        );

        // 4. Nothing anywhere → None.
        let empty_home = tmp.path().join("empty-home");
        std::fs::create_dir_all(&empty_home).unwrap();
        assert!(
            discover(&no_env, &tmp.path().join("no-such"), Some(&empty_home))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn discover_fails_loud_when_explicit_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("no-such-config.toml");
        let err = discover(
            &fake_env(&[("CONGA_CONFIG", missing.to_str().unwrap())]),
            Path::new("no-such"),
            None,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("CONGA_CONFIG"), "must name the var: {msg}");
        assert!(
            msg.contains("no-such-config.toml"),
            "must name the path: {msg}"
        );
    }

    #[test]
    fn shipped_example_file_parses() {
        // vendor 副本不携带上游的示例配置文件；带上时才校验其可解析。
        let Ok(raw) =
            std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/config.example.toml"))
        else {
            return;
        };
        let cfg: ConfigFile =
            toml::from_str(&raw).expect("example must parse (unknown key = drift)");
        // Sanity: the example actually configures the required LLM trio.
        let names = cfg.env_pairs();
        for k in ["CONGA_LLM_BASE_URL", "CONGA_LLM_KEY", "CONGA_LLM_MODEL"] {
            assert!(names.iter().any(|(n, _)| n == k), "missing {k}");
        }
    }
}
