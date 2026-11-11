//! 插件 manifest（`plugin.toml`）解析与闭集校验。
//!
//! manifest 是**宿主在 spawn 之前读的声明**（发现/调色板/配置表单/参数
//! 校验），插件进程自己不读它——initialize 携带的是已解析的快照。解析与
//! 合并分家：本模块只做「这份声明合法吗」，三源合并（manifest default +
//! journal 值 + secret 引用）只在 flow-backend 的一处做。
//!
//! 校验是闭集纪律的执行点（设计见 docs/plans/2026-11-06-plugin-ui.md
//! §2.2）：widget、code format 都是闭集；加值 = 宿主版本，不问插件。
//! 解析失败 = 该插件禁用 + 宿主醒目日志，**不猜、不跳过**。
//!
//! 兼容性（同 wire.md 纪律）：加字段兼容（未知字段忽略）；改字段语义 =
//! 宿主解析器主版本。

use serde::{Deserialize, Serialize};

/// manifest 解析错误：消息自带定位（哪个字段、为什么），宿主原样进日志。
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ManifestError(pub String);

/// 解析并校验一份 `plugin.toml` 文本。
pub fn parse(text: &str) -> Result<PluginManifest, ManifestError> {
    let raw: RawManifest =
        toml::from_str(text).map_err(|error| ManifestError(format!("toml 解析失败：{error}")))?;
    let manifest = PluginManifest::from_raw(raw)?;
    manifest.validate()?;
    Ok(manifest)
}

/// 已解析且通过校验的插件 manifest。
#[derive(Debug, Clone, PartialEq)]
pub struct PluginManifest {
    /// `[plugin]`：身份与入口。
    pub plugin: PluginSection,
    /// `[[config.field]]`：配置声明（三源合并的 schema 侧输入）。
    pub config_fields: Vec<FieldDecl>,
    /// `[ui]`：声明消费（调色板 / 参数表单 / 运行视图）。
    pub ui: UiSection,
    /// `[methods]`：runtime 可调方法。
    pub methods: MethodsSection,
    /// `[permissions]`：能力需求上限声明（09 §7）。缺省 = legacy-unrestricted
    /// （不伪装为空能力；执行需管理员显式确认）。声明只是需求上限，
    /// 实际允许范围 = 声明 ∩ 安装授予 ∩ run 策略 ∩ 逐次裁决。
    pub permissions: ManifestPermissions,
}

/// `[permissions]` 的解析结果。
#[derive(Debug, Clone, PartialEq)]
pub enum ManifestPermissions {
    /// 未声明：无限制执行的旧插件。授予页必须显式确认风险后才能用于新 run；
    /// Plan 模式一律拒绝。
    Legacy,
    /// 已声明的能力需求（闭集）。
    Declared(PermissionsSection),
}

/// 权限声明段（schema=1）。
#[derive(Debug, Clone, PartialEq)]
pub struct PermissionsSection {
    pub schema: u32,
    /// 宿主/执行器/插件三方握手的能力要求（当前闭集：master-permit-v1）。
    pub required_features: Vec<String>,
    pub capabilities: Vec<ManifestCapability>,
}

/// 一条 manifest 能力声明：matcher + required（未授予的 required 能力使
/// 插件不可执行；optional 可缺失，插件自行处理）。
#[derive(Debug, Clone, PartialEq)]
pub struct ManifestCapability {
    pub capability: flow_dto::permission::Capability,
    pub required: bool,
}

/// `[plugin]` 段。
#[derive(Debug, Clone, PartialEq)]
pub struct PluginSection {
    /// 全局唯一 id（kebab-case；= 插件目录名 = RPC 命名空间）。
    pub id: String,
    /// 展示名（缺省 = id）。
    pub name: String,
    /// 插件版本（SemVer 建议，不强制解析）。
    pub version: String,
    /// 入口二进制：相对插件目录或绝对路径。存在性由宿主在发现期检查
    ///（解析器只见文本）。
    pub entry: String,
    /// plugin-proto 协议版本（廉价快筛；握手再权威裁决）。
    pub protocol: u32,
    /// 本插件实现的节点类型（runtime 路由键）。内置类型名出现于此 =
    /// 宿主拒绝加载（防语义被插件悄悄顶掉）——该检查在 runtime 侧，
    /// 解析器不知道内置集。
    pub node_types: Vec<String>,
}

/// 一个字段声明（`[[config.field]]` / `[[ui.params.field]]` 共用）。
#[derive(Debug, Clone, PartialEq)]
pub struct FieldDecl {
    /// 字段键名。
    pub key: String,
    /// widget（闭集）。
    pub widget: Widget,
    /// 必填 = 存在且非空。
    pub required: bool,
    /// 缺省值（字符串形态；类型归属由 widget 决定，合并时转换）。
    pub default: Option<String>,
}

/// widget 闭集（v1.1：textarea 已并入 `code{format:text}`）。
#[derive(Debug, Clone, PartialEq)]
pub enum Widget {
    /// 单行文本（`max_len`）。
    Text { max_len: Option<usize> },
    /// 代码/多行文本（`format` 区分编辑器高亮；`text` 即普通多行）。
    Code {
        format: CodeFormat,
        max_len: Option<usize>,
    },
    /// 数值（闭区间约束）。
    Number { min: Option<f64>, max: Option<f64> },
    /// 布尔。
    Boolean,
    /// 枚举（values 非空）。
    Enum { values: Vec<String> },
    /// 秘密引用：指向 secrets 存储的 key，永不承载明文。
    Secret,
}

/// code 格式闭集。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodeFormat {
    Text,
    Js,
    Json,
    Shell,
}

/// `[ui]` 段（声明消费；runtime 不渲染，master/web 消费）。
#[derive(Debug, Clone, PartialEq, Default)]
pub struct UiSection {
    /// 调色板元数据。
    pub palette: Option<PaletteDecl>,
    /// 各节点类型的参数表单。
    pub params_forms: Vec<ParamsFormDecl>,
    /// 运行详情展示键。
    pub run_view: Option<RunViewDecl>,
}

/// 调色板声明：宿主分类/图标字典的 key（不是资源文件，视觉注入在
/// 字典层挡掉）。
#[derive(Debug, Clone, PartialEq)]
pub struct PaletteDecl {
    pub category: String,
    pub icon_key: String,
}

/// 一个节点类型的参数表单。
#[derive(Debug, Clone, PartialEq)]
pub struct ParamsFormDecl {
    /// 目标节点类型（必须在 `node_types` 内——跨插件语义 = 路由分叉）。
    pub node_type: String,
    pub fields: Vec<FieldDecl>,
}

/// 运行详情声明。
#[derive(Debug, Clone, PartialEq)]
pub struct RunViewDecl {
    /// outcome 展示键：单级 dot-path（如 `agent.model`），缺失跳过，
    /// 不遍历数组。
    pub summary_keys: Vec<String>,
}

/// `[methods]` 段。
#[derive(Debug, Clone, PartialEq)]
pub struct MethodsSection {
    /// 节点执行入口方法（必填：runtime 按它派发）。
    pub invoke: String,
}

/// UI 投影：run 快照冻结的本体（不是版本号——插件升级/卸载后历史 run
/// 仍要能渲染，见 UI 设计 §4）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UiProjection {
    /// 插件 id（展示归属）。
    pub plugin_id: String,
    /// 插件版本（诊断展示；渲染不依赖它回读任何东西）。
    pub version: String,
    /// 调色板元数据（可缺省）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub palette: Option<PaletteDecl>,
    /// 参数表单（可缺省：raw JSON 兜底永远可用）。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub params_forms: Vec<ParamsFormDecl>,
    /// 运行详情展示键（可缺省）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_view: Option<RunViewDecl>,
}

impl PluginManifest {
    /// 权限声明投影（run 快照冻结物，09 §7.2 `PermissionProjection` 的内容）：
    /// 规范化需求 + required，不含授予（授予记录只写 journal）。
    pub fn permissions_projection(&self) -> Option<serde_json::Value> {
        let ManifestPermissions::Declared(section) = &self.permissions else {
            return None;
        };
        let capabilities: Vec<serde_json::Value> = section
            .capabilities
            .iter()
            .map(|entry| {
                let mut value = entry.capability.to_json();
                value["required"] = serde_json::json!(entry.required);
                value
            })
            .collect();
        Some(serde_json::json!({
            "schema": section.schema,
            "required_features": section.required_features,
            "capabilities": capabilities,
        }))
    }

    /// 生成 UI 投影（快照冻结物）。
    pub fn ui_projection(&self) -> UiProjection {
        UiProjection {
            plugin_id: self.plugin.id.clone(),
            version: self.plugin.version.clone(),
            palette: self.ui.palette.clone(),
            params_forms: self.ui.params_forms.clone(),
            run_view: self.ui.run_view.clone(),
        }
    }

    fn validate(&self) -> Result<(), ManifestError> {
        let PluginSection {
            id,
            name: _,
            version,
            entry,
            protocol: _,
            node_types,
        } = &self.plugin;
        if !is_kebab_id(id) {
            return Err(ManifestError(format!(
                "plugin.id 必须是 kebab-case（当前 {id:?}）"
            )));
        }
        if version.trim().is_empty() {
            return Err(ManifestError("plugin.version 不能为空".into()));
        }
        if entry.trim().is_empty() {
            return Err(ManifestError("plugin.entry 不能为空".into()));
        }
        if node_types.is_empty() {
            return Err(ManifestError(
                "plugin.node_types 不能为空：没有节点类型的插件没有意义".into(),
            ));
        }
        for node_type in node_types {
            if !is_node_type_name(node_type) {
                return Err(ManifestError(format!(
                    "node_types 含非法名 {node_type:?}（允许小写字母/数字/-/_，字母开头）"
                )));
            }
        }
        if self.methods.invoke.trim().is_empty() {
            return Err(ManifestError(
                "methods.invoke 必填：runtime 按它派发节点执行".into(),
            ));
        }
        let known = &self.plugin.node_types;
        for form in &self.ui.params_forms {
            if !known.contains(&form.node_type) {
                return Err(ManifestError(format!(
                    "ui.params.for={:?} 不在 node_types 内（跨插件表单 = 路由分叉）",
                    form.node_type
                )));
            }
        }
        if let Some(run_view) = &self.ui.run_view {
            for key in &run_view.summary_keys {
                let dots = key.matches('.').count();
                if dots > 1 || key.trim().is_empty() || key.starts_with('.') || key.ends_with('.') {
                    return Err(ManifestError(format!(
                        "summary_keys 含非法路径 {key:?}：单级 dot-path，段非空"
                    )));
                }
            }
        }
        let mut config_keys = std::collections::BTreeSet::new();
        for field in self
            .config_fields
            .iter()
            .chain(self.ui.params_forms.iter().flat_map(|form| &form.fields))
        {
            if field.key.trim().is_empty() {
                return Err(ManifestError("字段 key 不能为空".into()));
            }
            if !config_keys.insert((&field.key, true)) {
                return Err(ManifestError(format!("字段 key 重复：{:?}", field.key)));
            }
            match &field.widget {
                Widget::Enum { values } if values.is_empty() => {
                    return Err(ManifestError(format!(
                        "enum 字段 {:?} 的 values 不能为空",
                        field.key
                    )));
                }
                Widget::Number { min, max } => {
                    if let (Some(min), Some(max)) = (min, max) {
                        if min > max {
                            return Err(ManifestError(format!(
                                "number 字段 {:?} 的 min > max",
                                field.key
                            )));
                        }
                    }
                }
                _ => {}
            }
        }
        self.validate_permissions()?;
        Ok(())
    }

    /// 权限声明的闭集校验（09 §7.1）：未知握手要求、重复 id、secret 引用
    /// 归属（config.*/params.* 必须指向本插件声明的 Widget::Secret 字段）。
    fn validate_permissions(&self) -> Result<(), ManifestError> {
        let ManifestPermissions::Declared(section) = &self.permissions else {
            return Ok(());
        };
        const KNOWN_FEATURES: [&str; 1] = ["master-permit-v1"];
        for feature in &section.required_features {
            if !KNOWN_FEATURES.contains(&feature.as_str()) {
                return Err(ManifestError(format!(
                    "[permissions].required_features 含未知能力 {feature:?}（闭集 master-permit-v1）"
                )));
            }
        }
        // secret 字段集合：config 与 params 表单里声明为 Widget::Secret 的 key。
        let mut secret_keys = std::collections::BTreeSet::new();
        for field in self
            .config_fields
            .iter()
            .chain(self.ui.params_forms.iter().flat_map(|form| &form.fields))
        {
            if matches!(field.widget, Widget::Secret) {
                secret_keys.insert(field.key.clone());
            }
        }
        let mut ids = std::collections::BTreeSet::new();
        for entry in &section.capabilities {
            let id = &entry.capability.id;
            if !ids.insert(id.clone()) {
                return Err(ManifestError(format!(
                    "[permissions] 能力 id 重复：{id}"
                )));
            }
            if let flow_dto::permission::ResourcePattern::Secret { names } = &entry.capability.pattern
            {
                for name in names {
                    // 引用形式 config.<key> / params.<key> 必须指向本插件声明的
                    // secret 字段；其余按直接密钥名（manifest 不硬编码用户密钥名，
                    // 直接名只用于宿主预置密钥）。
                    if let Some(key) = name
                        .strip_prefix("config.")
                        .or_else(|| name.strip_prefix("params."))
                    {
                        if !secret_keys.contains(key) {
                            return Err(ManifestError(format!(
                                "[permissions] secret 引用 {name:?} 指向的字段不是本插件声明的 Secret 字段"
                            )));
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

fn is_kebab_id(id: &str) -> bool {
    !id.is_empty()
        && id.split('-').all(|part| {
            !part.is_empty()
                && part
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        })
}

fn is_node_type_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
}

// ---------------------------------------------------------------------------
// TOML 形状（serde 中转层：字段名与闭集在这里翻译，校验在上面）
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct RawManifest {
    #[serde(default)]
    plugin: Option<RawPluginSection>,
    #[serde(default, rename = "config")]
    config: Option<RawConfigSection>,
    #[serde(default)]
    ui: Option<RawUiSection>,
    #[serde(default)]
    methods: Option<RawMethodsSection>,
    #[serde(default)]
    permissions: Option<RawPermissionsSection>,
}

/// `[permissions]` 的 TOML 形状。能力条目经 JSON 中转走 flow-dto 的闭集
/// 解析（`parse_manifest_capability`），字段闭集只有一份实现。
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPermissionsSection {
    #[serde(default)]
    schema: Option<u32>,
    #[serde(default, rename = "required_features")]
    required_features: Vec<String>,
    #[serde(default)]
    capabilities: Vec<toml::Table>,
}

#[derive(Debug, Deserialize)]
struct RawPluginSection {
    id: String,
    #[serde(default)]
    name: Option<String>,
    version: String,
    entry: String,
    protocol: u32,
    #[serde(default)]
    node_types: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize)]
struct RawConfigSection {
    #[serde(default, rename = "field")]
    fields: Vec<RawField>,
}

#[derive(Debug, Default, Deserialize)]
struct RawUiSection {
    #[serde(default)]
    palette: Option<RawPalette>,
    #[serde(default, rename = "params")]
    params_forms: Vec<RawParamsForm>,
    #[serde(default, rename = "run_view")]
    run_view: Option<RawRunView>,
}

#[derive(Debug, Deserialize)]
struct RawPalette {
    category: String,
    #[serde(default)]
    icon_key: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawParamsForm {
    #[serde(rename = "for")]
    node_type: String,
    #[serde(default, rename = "field")]
    fields: Vec<RawField>,
}

#[derive(Debug, Deserialize)]
struct RawRunView {
    #[serde(default, rename = "summary_keys")]
    summary_keys: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct RawMethodsSection {
    #[serde(default)]
    invoke: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawField {
    key: String,
    #[serde(rename = "type")]
    widget: String,
    #[serde(default)]
    format: Option<String>,
    #[serde(default)]
    required: bool,
    #[serde(default)]
    default: Option<String>,
    #[serde(default)]
    min: Option<f64>,
    #[serde(default)]
    max: Option<f64>,
    #[serde(default, rename = "max_len")]
    max_len: Option<usize>,
    #[serde(default)]
    values: Option<Vec<String>>,
}

impl PluginManifest {
    fn from_raw(raw: RawManifest) -> Result<Self, ManifestError> {
        let plugin = raw
            .plugin
            .ok_or_else(|| ManifestError("缺少 [plugin] 段：manifest 的身份段不可省略".into()))?;
        let widget = |field: &RawField| -> Result<Widget, ManifestError> {
            match field.widget.as_str() {
                "text" => Ok(Widget::Text {
                    max_len: field.max_len,
                }),
                "code" => {
                    let format = match field.format.as_deref() {
                        None | Some("text") => CodeFormat::Text,
                        Some("js") => CodeFormat::Js,
                        Some("json") => CodeFormat::Json,
                        Some("shell") => CodeFormat::Shell,
                        Some(other) => {
                            return Err(ManifestError(format!(
                                "字段 {:?} 的 code format 非法：{other:?}（闭集 text/js/json/shell）",
                                field.key
                            )))
                        }
                    };
                    Ok(Widget::Code {
                        format,
                        max_len: field.max_len,
                    })
                }
                "number" => Ok(Widget::Number {
                    min: field.min,
                    max: field.max,
                }),
                "boolean" => Ok(Widget::Boolean),
                "enum" => Ok(Widget::Enum {
                    values: field.values.clone().unwrap_or_default(),
                }),
                "secret" => Ok(Widget::Secret),
                other => Err(ManifestError(format!(
                    "字段 {:?} 的 type 非法：{other:?}（闭集 text/code/number/boolean/enum/secret）",
                    field.key
                ))),
            }
        };
        let fields = |raw_fields: &[RawField]| -> Result<Vec<FieldDecl>, ManifestError> {
            raw_fields
                .iter()
                .map(|field| {
                    Ok(FieldDecl {
                        key: field.key.clone(),
                        widget: widget(field)?,
                        required: field.required,
                        default: field.default.clone(),
                    })
                })
                .collect()
        };
        let ui_raw = raw.ui.unwrap_or_default();
        let mut params_forms = Vec::with_capacity(ui_raw.params_forms.len());
        for form in &ui_raw.params_forms {
            params_forms.push(ParamsFormDecl {
                node_type: form.node_type.clone(),
                fields: fields(&form.fields)?,
            });
        }
        // [permissions]：经 JSON 中转走 flow-dto 闭集解析；缺段 = legacy。
        let permissions = match raw.permissions {
            None => ManifestPermissions::Legacy,
            Some(section) => {
                let schema = section.schema.ok_or_else(|| {
                    ManifestError("[permissions].schema 必填（当前支持 1）".into())
                })?;
                if schema != flow_dto::permission::PERMISSION_SCHEMA_VERSION {
                    return Err(ManifestError(format!(
                        "[permissions].schema = {schema} 不受支持（当前支持 1）"
                    )));
                }
                let mut capabilities = Vec::with_capacity(section.capabilities.len());
                for table in &section.capabilities {
                    let value = serde_json::to_value(table)
                        .map_err(|e| ManifestError(format!("能力条目转码失败：{e}")))?;
                    let capability = flow_dto::permission::parse_manifest_capability(&value)
                        .map_err(|error| ManifestError(format!("[permissions] {error}")))?;
                    let required = table
                        .get("required")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(true);
                    capabilities.push(ManifestCapability {
                        capability,
                        required,
                    });
                }
                ManifestPermissions::Declared(PermissionsSection {
                    schema,
                    required_features: section.required_features,
                    capabilities,
                })
            }
        };
        Ok(Self {
            config_fields: fields(&raw.config.unwrap_or_default().fields)?,
            permissions,
            plugin: PluginSection {
                id: plugin.id,
                name: plugin.name.unwrap_or_default(),
                version: plugin.version,
                entry: plugin.entry,
                protocol: plugin.protocol,
                node_types: plugin.node_types.unwrap_or_default(),
            },
            ui: UiSection {
                palette: ui_raw.palette.map(|palette| PaletteDecl {
                    category: palette.category,
                    icon_key: palette.icon_key.unwrap_or_default(),
                }),
                params_forms,
                run_view: ui_raw.run_view.map(|view| RunViewDecl {
                    summary_keys: view.summary_keys.unwrap_or_default(),
                }),
            },
            methods: MethodsSection {
                invoke: raw
                    .methods
                    .and_then(|methods| methods.invoke)
                    .unwrap_or_default(),
            },
        })
    }
}

// PaletteDecl / ParamsFormDecl / RunViewDecl 参与快照投影序列化。
impl Serialize for PaletteDecl {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("PaletteDecl", 2)?;
        state.serialize_field("category", &self.category)?;
        state.serialize_field("icon_key", &self.icon_key)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for PaletteDecl {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Helper {
            category: String,
            #[serde(default)]
            icon_key: String,
        }
        let helper = Helper::deserialize(deserializer)?;
        Ok(Self {
            category: helper.category,
            icon_key: helper.icon_key,
        })
    }
}

impl Serialize for ParamsFormDecl {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("ParamsFormDecl", 2)?;
        state.serialize_field("for", &self.node_type)?;
        let fields: Vec<FieldDeclSerde> = self
            .fields
            .iter()
            .map(|f| FieldDeclSerde(f.clone()))
            .collect();
        state.serialize_field("fields", &fields)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for ParamsFormDecl {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Helper {
            #[serde(rename = "for")]
            node_type: String,
            #[serde(default)]
            fields: Vec<FieldDeclSerde>,
        }
        let helper = Helper::deserialize(deserializer)?;
        Ok(Self {
            node_type: helper.node_type,
            fields: helper.fields.into_iter().map(|f| f.0).collect(),
        })
    }
}

impl Serialize for RunViewDecl {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("RunViewDecl", 1)?;
        state.serialize_field("summary_keys", &self.summary_keys)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for RunViewDecl {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Helper {
            #[serde(default)]
            summary_keys: Vec<String>,
        }
        let helper = Helper::deserialize(deserializer)?;
        Ok(Self {
            summary_keys: helper.summary_keys,
        })
    }
}

/// FieldDecl 的投影形态（序列化用：widget 展平为 type/format + 属性）。
#[derive(Debug)]
struct FieldDeclSerde(FieldDecl);

impl Serialize for FieldDeclSerde {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let FieldDecl {
            key,
            widget,
            required,
            default,
        } = &self.0;
        let extra = match widget {
            Widget::Text { max_len } => usize::from(max_len.is_some()),
            Widget::Code { max_len, .. } => 1 + usize::from(max_len.is_some()),
            Widget::Number { min, max } => usize::from(min.is_some()) + usize::from(max.is_some()),
            Widget::Enum { .. } => 1,
            Widget::Boolean | Widget::Secret => 0,
        };
        let mut state = serializer.serialize_struct("FieldDecl", 4 + extra)?;
        state.serialize_field("key", key)?;
        match widget {
            Widget::Text { max_len } => {
                state.serialize_field("type", "text")?;
                if let Some(len) = max_len {
                    state.serialize_field("max_len", len)?;
                }
            }
            Widget::Code { format, max_len } => {
                state.serialize_field("type", "code")?;
                state.serialize_field(
                    "format",
                    match format {
                        CodeFormat::Text => "text",
                        CodeFormat::Js => "js",
                        CodeFormat::Json => "json",
                        CodeFormat::Shell => "shell",
                    },
                )?;
                if let Some(len) = max_len {
                    state.serialize_field("max_len", len)?;
                }
            }
            Widget::Number { min, max } => {
                state.serialize_field("type", "number")?;
                if let Some(min) = min {
                    state.serialize_field("min", min)?;
                }
                if let Some(max) = max {
                    state.serialize_field("max", max)?;
                }
            }
            Widget::Boolean => state.serialize_field("type", "boolean")?,
            Widget::Enum { values } => {
                state.serialize_field("type", "enum")?;
                state.serialize_field("values", values)?;
            }
            Widget::Secret => state.serialize_field("type", "secret")?,
        }
        state.serialize_field("required", required)?;
        if let Some(default) = default {
            state.serialize_field("default", default)?;
        }
        state.end()
    }
}

impl<'de> Deserialize<'de> for FieldDeclSerde {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Helper {
            key: String,
            #[serde(rename = "type")]
            widget: String,
            #[serde(default)]
            format: Option<String>,
            #[serde(default)]
            required: bool,
            #[serde(default)]
            default: Option<String>,
            #[serde(default)]
            min: Option<f64>,
            #[serde(default)]
            max: Option<f64>,
            #[serde(default)]
            max_len: Option<usize>,
            #[serde(default)]
            values: Option<Vec<String>>,
        }
        let helper = Helper::deserialize(deserializer)?;
        let widget = match (helper.widget.as_str(), helper.format.as_deref()) {
            ("text", _) => Widget::Text {
                max_len: helper.max_len,
            },
            ("code", None | Some("text")) => Widget::Code {
                format: CodeFormat::Text,
                max_len: helper.max_len,
            },
            ("code", Some("js")) => Widget::Code {
                format: CodeFormat::Js,
                max_len: helper.max_len,
            },
            ("code", Some("json")) => Widget::Code {
                format: CodeFormat::Json,
                max_len: helper.max_len,
            },
            ("code", Some("shell")) => Widget::Code {
                format: CodeFormat::Shell,
                max_len: helper.max_len,
            },
            ("code", Some(other)) => {
                return Err(serde::de::Error::custom(format!(
                    "code format 非法：{other:?}"
                )))
            }
            ("number", _) => Widget::Number {
                min: helper.min,
                max: helper.max,
            },
            ("boolean", _) => Widget::Boolean,
            ("enum", _) => Widget::Enum {
                values: helper.values.unwrap_or_default(),
            },
            ("secret", _) => Widget::Secret,
            (other, _) => return Err(serde::de::Error::custom(format!("widget 非法：{other:?}"))),
        };
        Ok(Self(FieldDecl {
            key: helper.key,
            widget,
            required: helper.required,
            default: helper.default,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
[plugin]
id = "harness-agent"
name = "Harness Agent"
version = "0.1.0"
entry = "bin/flow-agent-plugin"
protocol = 1
node_types = ["harness"]

[methods]
invoke = "agent/runTurn"

[[config.field]]
key = "max_turns"
type = "number"
default = "8"
min = 1
max = 64

[[config.field]]
key = "api_key"
type = "secret"
required = true

[ui]
[ui.palette]
category = "ai"
icon_key = "spark"

[[ui.params]]
for = "harness"
[[ui.params.field]]
key = "prompt"
type = "code"
format = "text"
required = true
max_len = 65536
[[ui.params.field]]
key = "mode"
type = "enum"
values = ["agent", "command"]
default = "command"

[ui.run_view]
summary_keys = ["agent.model", "exit_code"]
"#;

    #[test]
    fn parses_valid_manifest_with_projection() {
        let manifest = parse(VALID).expect("valid manifest");
        assert_eq!(manifest.plugin.id, "harness-agent");
        assert_eq!(manifest.plugin.node_types, vec!["harness".to_string()]);
        assert_eq!(manifest.methods.invoke, "agent/runTurn");
        assert_eq!(manifest.config_fields.len(), 2);
        assert!(matches!(
            manifest.config_fields[0].widget,
            Widget::Number {
                min: Some(1.0),
                max: Some(64.0)
            }
        ));
        assert!(matches!(manifest.config_fields[1].widget, Widget::Secret));

        // 投影可序列化（快照冻结物），且往返不丢字段。
        let projection = manifest.ui_projection();
        let json = serde_json::to_value(&projection).unwrap();
        assert_eq!(json["palette"]["category"], "ai");
        assert_eq!(json["params_forms"][0]["for"], "harness");
        assert_eq!(json["run_view"]["summary_keys"][0], "agent.model");
        let back: UiProjection = serde_json::from_value(json).unwrap();
        assert_eq!(back, projection);
    }

    #[test]
    fn rejects_closed_set_violations() {
        let bad_widget = VALID.replace(r#""secret""#, r#""color""#);
        assert!(parse(&bad_widget).is_err(), "widget 闭集");

        let bad_format = VALID.replace("format = \"text\"", "format = \"yaml\"");
        assert!(parse(&bad_format).is_err(), "code format 闭集");

        let empty_enum = VALID.replace("values = [\"agent\", \"command\"]", "values = []");
        assert!(parse(&empty_enum).is_err(), "enum values 非空");

        let min_max = VALID.replace("min = 1\nmax = 64", "min = 10\nmax = 4");
        assert!(parse(&min_max).is_err(), "min <= max");
    }

    #[test]
    fn rejects_identity_and_structure_violations() {
        assert!(
            parse(&VALID.replace("id = \"harness-agent\"", "id = \"Harness_Agent\"")).is_err(),
            "id 必须 kebab-case"
        );
        assert!(
            parse(&VALID.replace("node_types = [\"harness\"]", "node_types = []")).is_err(),
            "node_types 非空"
        );
        assert!(
            parse(&VALID.replace("invoke = \"agent/runTurn\"", "invoke = \"\"")).is_err(),
            "invoke 必填"
        );
        // 表单指向未声明的节点类型 = 跨插件语义，拒绝。
        assert!(
            parse(&VALID.replace("for = \"harness\"", "for = \"script\"")).is_err(),
            "params.for 必须在 node_types 内"
        );
        // summary_keys 单级 dot-path。
        assert!(
            parse(&VALID.replace(
                "summary_keys = [\"agent.model\", \"exit_code\"]",
                "summary_keys = [\"a.b.c\"]",
            ))
            .is_err(),
            "summary_keys 最多一级嵌套"
        );
        // 缺 [plugin] 段。
        assert!(
            parse("[methods]\ninvoke = \"x\"\n").is_err(),
            "[plugin] 必需"
        );
    }
}
