use std::time::{Duration, Instant};

use rquickjs::function::Func;
use rquickjs::{Array, BigInt, Context, Ctx, Function, Object, Runtime, Value as JsValue};
use serde_json::Value;

use crate::error::EngineError;
use crate::event::{LogLevel, LogStream};
use crate::nodelog::NodeLogger;

/// JS → Rust 递归转换的最大深度：挡手工构造的深链/环，与 JSON.stringify 的
/// 「环即抛错」同类——报错退出，不爆 Rust 栈。
const MAX_JS_DEPTH: usize = 128;

/// 2^53：f64 能精确表示的最大整数（±9007199254740992）。
const MAX_EXACT_F64_INT: i64 = 9_007_199_254_740_992;

/// serde_json::Value → JS 原生值：输入面直接以 JS 对象/数组/标量注入沙箱，
/// 不再 JSON.stringify 成字符串再让 JS 端 JSON.parse 倒一手。
///
/// `define` 是预置的 Object.defineProperty 包装：与 JSON.parse 一致地创建
/// 「自有可枚举数据属性」。Object.set 走 setter 通道，键恰为 "__proto__" 时
/// 会改原型而不是建属性，与 JSON.parse 的语义分叉。
///
/// 大整数走 BigInt：|v| > 2^53 的整数经 JS Number（f64）会静默舍入
/// （9007199254740993 → …992）并沿节点输出向下游传播，转 BigInt 进出
/// 沙箱后透传/比较/模板展开精确无损。代价是脚本中 BigInt 与 Number
/// 混算会抛 TypeError——响亮失败好过静默错值（DESIGN §10）。
fn json_to_js<'js>(
    ctx: &Ctx<'js>,
    value: &Value,
    define: &Function<'js>,
) -> Result<JsValue<'js>, rquickjs::Error> {
    Ok(match value {
        Value::Null => JsValue::new_null(ctx.clone()),
        Value::Bool(b) => JsValue::new_bool(ctx.clone(), *b),
        Value::Number(n) => match (n.as_i64(), n.as_u64()) {
            // 超出 f64 精确整数域的 i64：BigInt 进沙箱，出口精确还原
            (Some(v), _) if !(-MAX_EXACT_F64_INT..=MAX_EXACT_F64_INT).contains(&v) => {
                BigInt::from_i64(ctx.clone(), v)?.into_value()
            }
            (Some(v), _) => JsValue::new_number(ctx.clone(), v as f64),
            // serde_json 对超 i64 的正整数存 u64：必然 > 2^53，一律 BigInt
            (None, Some(v)) => BigInt::from_u64(ctx.clone(), v)?.into_value(),
            (None, None) => JsValue::new_number(ctx.clone(), n.as_f64().unwrap_or(f64::NAN)),
        },
        Value::String(s) => rquickjs::String::from_str(ctx.clone(), s)?.into_value(),
        Value::Array(items) => {
            let array = Array::new(ctx.clone())?;
            for (i, item) in items.iter().enumerate() {
                array.set(i, json_to_js(ctx, item, define)?)?;
            }
            array.into()
        }
        Value::Object(map) => {
            let object = Object::new(ctx.clone())?;
            for (k, v) in map {
                let child = json_to_js(ctx, v, define)?;
                define.call::<_, ()>((object.as_value().clone(), k.as_str(), child))?;
            }
            object.into()
        }
    })
}

/// JS 求值结果读取器：把 JS 原生值转回 serde_json::Value，语义对齐
/// JSON.stringify（改造前的结果出口），包括但不限于：
/// - NaN/Infinity → null；
/// - BigInt → 按 i64/u64 精确还原（大整数边界，见 `json_to_js`）；超出
///   JSON 整数域的 BigInt 报错（stringify 同样抛 TypeError）；
/// - 整数值浮点 → 整数（stringify(2.0) 产出 "2" 而非 "2.0"）；
/// - Date（含嵌套位）按 toJSON 约定出 ISO 字符串；
/// - function/symbol/undefined 被丢弃（对象字段缺键、数组成员为 null）；
///
/// `None` 即「该值不可序列化」的传递信使。
struct JsReader<'js> {
    date_ctor: Object<'js>,
    date_to_iso: Function<'js>,
    bigint_to_string: Function<'js>,
    remaining: std::cell::Cell<usize>,
    max_depth: usize,
}

impl<'js> JsReader<'js> {
    fn new(ctx: &Ctx<'js>) -> Result<Self, rquickjs::Error> {
        Ok(Self {
            date_ctor: ctx.globals().get::<_, Object>("Date")?,
            date_to_iso: ctx.eval::<Function, _>("(function (d) { return d.toISOString(); })")?,
            bigint_to_string: ctx.eval::<Function, _>("(function (b) { return b.toString(); })")?,
            remaining: std::cell::Cell::new(usize::MAX),
            max_depth: MAX_JS_DEPTH,
        })
    }

    /// BigInt → JSON 整数：经十进制字符串回读后按 i64/u64 精确还原。
    /// rquickjs 的 `to_i64` 对超范围值按 JS 语义静默回绕，不能用它判界；
    /// 超出 JSON 整数域（i64/u64）的 BigInt 响亮报错，绝不静默截断。
    fn bigint_to_json(&self, value: &JsValue<'js>) -> Result<Value, rquickjs::Error> {
        let digits: String = self.bigint_to_string.call((value.clone(),))?;
        if let Ok(v) = digits.parse::<i64>() {
            return Ok(Value::from(v));
        }
        if let Ok(v) = digits.parse::<u64>() {
            return Ok(Value::from(v));
        }
        Err(rquickjs::Error::new_from_js_message(
            "bigint",
            "JSON 整数",
            format!("BigInt 超出 i64/u64 范围，拒绝无损序列化：{digits}"),
        ))
    }

    fn to_json(
        &self,
        value: &JsValue<'js>,
        depth: usize,
    ) -> Result<Option<Value>, rquickjs::Error> {
        if depth > self.max_depth {
            return Err(rquickjs::Error::new_from_js(
                "object",
                "数据结构嵌套超过 128 层（或存在环），拒绝序列化",
            ));
        }
        self.charge(8)?;
        Ok(Some(match value.type_of() {
            rquickjs::Type::Null | rquickjs::Type::Uninitialized => Value::Null,
            rquickjs::Type::Bool => Value::Bool(value.get::<bool>()?),
            rquickjs::Type::Int => Value::from(value.get::<i32>()? as i64),
            rquickjs::Type::Float => {
                let f = value.get::<f64>()?;
                if !f.is_finite() {
                    Value::Null
                } else if f.fract() == 0.0 && f.abs() <= 9.007_199_254_740_992e15 {
                    // 2^53 是 f64 能精确表示的最大整数：stringify 对它输出整数形式
                    Value::from(f as i64)
                } else {
                    Value::from(f)
                }
            }
            rquickjs::Type::String => {
                let s = value.get::<String>()?;
                self.charge(s.len())?;
                Value::String(s)
            }
            rquickjs::Type::Array => {
                let array = value.get::<Array>()?;
                if array.len() > self.remaining.get() / 8 {
                    return Err(rquickjs::Error::new_from_js(
                        "array",
                        "output budget exceeded",
                    ));
                }
                let mut items = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    // 稀疏数组的洞读出来是 undefined：对应 stringify 的 [null]
                    let item = array.get::<JsValue>(i)?;
                    items.push(self.to_json(&item, depth + 1)?.unwrap_or(Value::Null));
                }
                Value::Array(items)
            }
            rquickjs::Type::BigInt => self.bigint_to_json(value)?,
            // stringify 丢弃不可序列化值；顶层已由包装器归 null，这里覆盖字段/数组位。
            // 注意 quickjs-ng 的 type_of 把普通函数报成 Constructor（该分支排在
            // Function 之前），构造器与函数一并丢弃。
            rquickjs::Type::Undefined
            | rquickjs::Type::Function
            | rquickjs::Type::Constructor
            | rquickjs::Type::Symbol => return Ok(None),
            // 普通对象、Date，以及 Promise/Exception/Proxy/Module
            // （stringify 视角下都是「按自有可枚举键走查的普通对象」）
            _ => {
                let object = value.get::<Object>()?;
                if object.is_instance_of(&self.date_ctor) {
                    // Date 的 toJSON 约定：ISO 字符串
                    let iso = self
                        .date_to_iso
                        .call::<_, String>((object.as_value().clone(),))?;
                    return Ok(Some(Value::String(iso)));
                }
                let mut map = serde_json::Map::new();
                for key in object.keys::<String>() {
                    let key = key?;
                    self.charge(key.len())?;
                    let item = object.get::<_, JsValue>(&key)?;
                    if let Some(json) = self.to_json(&item, depth + 1)? {
                        map.insert(key, json);
                    }
                }
                Value::Object(map)
            }
        }))
    }
    fn charge(&self, n: usize) -> Result<(), rquickjs::Error> {
        if self.remaining.get() == usize::MAX {
            return Ok(());
        }
        let remaining = self
            .remaining
            .get()
            .checked_sub(n)
            .ok_or_else(|| rquickjs::Error::new_from_js("value", "output budget exceeded"))?;
        self.remaining.set(remaining);
        Ok(())
    }
}

/// 表达式求值统一入口：JS 沙箱，无 IO，带超时中断。
///
/// 走 JS 而不是自造 DSL：前端预览和引擎求值共用同一种语言。
/// 每次求值都是全新的 Runtime + Context：全局表、原子表随运行期生命周期
/// 一起回收，节点之间不共享任何可变状态（决定论与隔离性）。
/// 不跨求值复用 Runtime 是刻意的：quickjs 的原子表只增不减，而复用后输入
/// 数据里的任意 JSON 键都会沉淀成原子——长生命周期进程下这是无界增长；
/// 把整个 Runtime 从「盘古开天」重新实例化，换回来的正是这张表的清零。
/// console.log → LogLevel 映射（与 Node 约定一致：error/warn 走 stderr）。
fn console_level(name: &str) -> LogLevel {
    match name {
        "debug" => LogLevel::Debug,
        "warn" => LogLevel::Warn,
        "error" => LogLevel::Error,
        _ => LogLevel::Info,
    }
}

fn console_stream(name: &str) -> LogStream {
    match name {
        "warn" | "error" => LogStream::Stderr,
        _ => LogStream::Stdout,
    }
}

fn run_js(
    body: &str,
    input: &Value,
    nodes: &Value,
    tpl: &Value,
    timeout: Duration,
    logger: &NodeLogger,
) -> Result<Value, EngineError> {
    run_js_limited(body, input, nodes, tpl, timeout, logger, None)
}

#[derive(Clone, Copy)]
pub struct JsLimits {
    pub input_bytes: usize,
    pub heap_bytes: usize,
    pub output_bytes: usize,
    pub depth: usize,
}
impl Default for JsLimits {
    fn default() -> Self {
        Self {
            input_bytes: 8 * 1024 * 1024,
            heap_bytes: 32 * 1024 * 1024,
            output_bytes: 8 * 1024 * 1024,
            depth: 64,
        }
    }
}

fn run_js_limited(
    body: &str,
    input: &Value,
    nodes: &Value,
    tpl: &Value,
    timeout: Duration,
    logger: &NodeLogger,
    limits: Option<JsLimits>,
) -> Result<Value, EngineError> {
    if let Some(limits) = limits {
        let mut remaining = limits.input_bytes;
        for value in [input, nodes, tpl] {
            flow_journal::codec::validate_depth(value, 0)
                .map_err(|e| EngineError::Expr(e.to_string()))?;
            let bytes = flow_journal::codec::bounded_json(value, remaining)
                .map_err(|e| EngineError::Expr(e.to_string()))?;
            remaining = remaining.saturating_sub(bytes.len());
        }
    }
    let runtime = Runtime::new().map_err(|e| EngineError::Expr(e.to_string()))?;
    if let Some(limits) = limits {
        runtime.set_memory_limit(limits.heap_bytes);
        runtime.set_max_stack_size(512 * 1024);
    }
    let deadline = Instant::now() + timeout;
    runtime.set_interrupt_handler(Some(Box::new(move || Instant::now() > deadline)));

    let context = Context::full(&runtime).map_err(|e| EngineError::Expr(e.to_string()))?;

    let program = format!(
        r#"(function () {{
  try {{
    var __v = (function () {{
{body}
    }})();
    return {{ ok: true, value: (__v === undefined ? null : __v) }};
  }} catch (e) {{
    return {{ ok: false, error: String((e && e.message) || e) }};
  }}
}})()"#
    );

    let outcome = context.with(|ctx| -> Result<Result<Value, String>, rquickjs::Error> {
        let globals = ctx.globals();
        // 与 JSON.parse 一致的属性定义（__proto__ 等键不会触发 setter）
        let define = ctx.eval::<Function, _>(
            "(function (o, k, v) { Object.defineProperty(o, k, { value: v, enumerable: true, writable: true, configurable: true }); })",
        )?;
        globals.set("input", json_to_js(&ctx, input, &define)?)?;
        globals.set("nodes", json_to_js(&ctx, nodes, &define)?)?;
        globals.set("tpl", json_to_js(&ctx, tpl, &define)?)?;

        // console 桥接：宿主函数收（级别，已格式化消息），格式化在 JS 侧完成
        //（多参数拼串、对象 JSON.stringify、循环引用回退 String()）。
        // 所有人都有 console：条件/模板求值传 disabled logger，引用 console
        // 不再 ReferenceError，只是没人听。
        let emit_logger = logger.clone();
        globals.set(
            "__flow_console_emit",
            Func::from(move |level: String, message: String| {
                emit_logger.log(console_level(&level), console_stream(&level), message);
            }),
        )?;
        let console: JsValue = ctx.eval(
            r#"(function (emit) {
  function fmt() {
    var out = [];
    for (var i = 0; i < arguments.length; i++) {
      var v = arguments[i];
      if (typeof v === 'string') { out.push(v); continue; }
      try { out.push(JSON.stringify(v)); } catch (e) { out.push(String(v)); }
    }
    return out.join(' ');
  }
  return {
    log: function () { emit('info', fmt.apply(null, arguments)); },
    info: function () { emit('info', fmt.apply(null, arguments)); },
    debug: function () { emit('debug', fmt.apply(null, arguments)); },
    warn: function () { emit('warn', fmt.apply(null, arguments)); },
    error: function () { emit('error', fmt.apply(null, arguments)); }
  };
})(__flow_console_emit)"#,
        )?;
        globals.set("console", console)?;

        let result: JsValue = ctx.eval(program)?;
        let outcome = result.get::<Object>()?;
        let mut reader = JsReader::new(&ctx)?;
        if let Some(limits)=limits {reader.remaining.set(limits.output_bytes);reader.max_depth=limits.depth;}
        if outcome.get::<_, bool>("ok")? {
            let value = outcome.get::<_, JsValue>("value")?;
            Ok(Ok(reader.to_json(&value, 0)?.unwrap_or(Value::Null)))
        } else {
            Ok(Err(outcome.get::<_, String>("error")?))
        }
    });
    match outcome.map_err(|e| EngineError::Expr(e.to_string()))? {
        Ok(value) => {
            if let Some(limits) = limits {
                flow_journal::codec::bounded_json(&value, limits.output_bytes)
                    .map_err(|e| EngineError::Expr(e.to_string()))?;
            }
            Ok(value)
        }
        Err(message) => Err(EngineError::Expr(message)),
    }
}

/// 脚本节点：body 是一段带 `return` 的函数体，可用 `input` 与 `nodes`。
/// `logger` 承接脚本内的 console.* 输出（level 透传，log/info→stdout，warn/error→stderr）。
pub fn eval_body(
    body: &str,
    input: &Value,
    nodes: &Value,
    timeout: Duration,
    logger: &NodeLogger,
) -> Result<Value, EngineError> {
    run_js(body, input, nodes, &Value::Null, timeout, logger)
}

pub fn eval_body_bounded(
    body: &str,
    input: &Value,
    nodes: &Value,
    timeout: Duration,
    logger: &NodeLogger,
) -> Result<Value, EngineError> {
    run_js_limited(
        body,
        input,
        nodes,
        &Value::Null,
        timeout,
        logger,
        Some(JsLimits::default()),
    )
}

/// 条件节点：求值单个表达式。console 可用（disabled logger，输出丢弃）。
pub fn eval_expr(
    expr: &str,
    input: &Value,
    nodes: &Value,
    timeout: Duration,
) -> Result<Value, EngineError> {
    let body = format!("    return ({expr});");
    run_js(
        &body,
        input,
        nodes,
        &Value::Null,
        timeout,
        &NodeLogger::disabled(),
    )
}

/// 展开字符串中的 `${expr}` 模板。所有节点 params 的统一前置展开
/// （`exec::expand_params`；script.code / condition.expr 等 opaque 字段除外）。
///
/// 两条规则，按「整个值是否恰为一个模板」分流：
///
/// 1. **整值模板**（`"${expr}"` 独占整个字符串）：按求值结果的**原类型**
///    替换——数字还是数字、对象还是对象。`input_mapping` 之类的结构化
///    传参依赖这条；`undefined` 归一为 `null`（与 `eval_body` 出口一致）。
/// 2. **插值模板**（模板嵌在更长文本里）：字符串语义，非字符串结果经
///    `JSON.stringify` 拼回原位（url/headers 的历史行为，逐字不变）。
///
/// 规则 1 只影响「整值 + 非字符串」这一格：整值字符串模板（如
/// `"${input.token}"`）在两条规则下结果相同，存量定义零行为变化。
pub fn expand_templates(
    tpl: &Value,
    input: &Value,
    nodes: &Value,
    timeout: Duration,
) -> Result<Value, EngineError> {
    expand_templates_impl(tpl, input, nodes, timeout, None)
}
pub fn expand_templates_bounded(
    tpl: &Value,
    input: &Value,
    nodes: &Value,
    timeout: Duration,
) -> Result<Value, EngineError> {
    expand_templates_impl(tpl, input, nodes, timeout, Some(JsLimits::default()))
}
fn expand_templates_impl(
    tpl: &Value,
    input: &Value,
    nodes: &Value,
    timeout: Duration,
    limits: Option<JsLimits>,
) -> Result<Value, EngineError> {
    let body = r#"    function __one(e) {
      var r = eval(e);
      return r === undefined ? null : r;
    }
    function __interp(v, input, nodes) {
      return v.replace(/\$\{([^}]+)\}/g, function (_, e) {
        var r = __one(e);
        // BigInt 走 toString 拿精确十进制（雪花 ID 拼 URL）：JSON.stringify
        // 对 BigInt 抛错，而 f64 老路径在这里是静默舍入。
        if (typeof r === 'bigint') { return r.toString(); }
        return (typeof r === 'string') ? r : JSON.stringify(r);
      });
    }
    function __expand(v, input, nodes) {
      if (typeof v === 'string') {
        var whole = /^\$\{([^}]+)\}$/.exec(v);
        return whole ? __one(whole[1]) : __interp(v, input, nodes);
      }
      if (Array.isArray(v)) { return v.map(function (x) { return __expand(x, input, nodes); }); }
      if (v && typeof v === 'object') {
        var o = {};
        Object.keys(v).forEach(function (k) { o[k] = __expand(v[k], input, nodes); });
        return o;
      }
      return v;
    }
    return __expand(tpl, input, nodes);"#;
    run_js_limited(
        body,
        input,
        nodes,
        tpl,
        timeout,
        &NodeLogger::disabled(),
        limits,
    )
}

#[cfg(test)]
mod tests;
