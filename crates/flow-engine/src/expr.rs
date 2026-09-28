use std::time::{Duration, Instant};

use rquickjs::{Array, Ctx, Context, Function, Object, Runtime, Value as JsValue};
use serde_json::Value;

use crate::error::EngineError;

/// JS → Rust 递归转换的最大深度：挡手工构造的深链/环，与 JSON.stringify 的
/// 「环即抛错」同类——报错退出，不爆 Rust 栈。
const MAX_JS_DEPTH: usize = 128;

/// serde_json::Value → JS 原生值：输入面直接以 JS 对象/数组/标量注入沙箱，
/// 不再 JSON.stringify 成字符串再让 JS 端 JSON.parse 倒一手。
///
/// `define` 是预置的 Object.defineProperty 包装：与 JSON.parse 一致地创建
/// 「自有可枚举数据属性」。Object.set 走 setter 通道，键恰为 "__proto__" 时
/// 会改原型而不是建属性，与 JSON.parse 的语义分叉。
fn json_to_js<'js>(
    ctx: &Ctx<'js>,
    value: &Value,
    define: &Function<'js>,
) -> Result<JsValue<'js>, rquickjs::Error> {
    Ok(match value {
        Value::Null => JsValue::new_null(ctx.clone()),
        Value::Bool(b) => JsValue::new_bool(ctx.clone(), *b),
        Value::Number(n) => JsValue::new_number(ctx.clone(), n.as_f64().unwrap_or(f64::NAN)),
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
/// - NaN/Infinity → null；BigInt → 报错（stringify 同样抛 TypeError）；
/// - 整数值浮点 → 整数（stringify(2.0) 产出 "2" 而非 "2.0"）；
/// - Date（含嵌套位）按 toJSON 约定出 ISO 字符串；
/// - function/symbol/undefined 被丢弃（对象字段缺键、数组成员为 null）；
///
/// `None` 即「该值不可序列化」的传递信使。
struct JsReader<'js> {
    date_ctor: Object<'js>,
    date_to_iso: Function<'js>,
}

impl<'js> JsReader<'js> {
    fn new(ctx: &Ctx<'js>) -> Result<Self, rquickjs::Error> {
        Ok(Self {
            date_ctor: ctx.globals().get::<_, Object>("Date")?,
            date_to_iso: ctx
                .eval::<Function, _>("(function (d) { return d.toISOString(); })")?,
        })
    }

    fn to_json(&self, value: &JsValue<'js>, depth: usize) -> Result<Option<Value>, rquickjs::Error> {
        if depth > MAX_JS_DEPTH {
            return Err(rquickjs::Error::new_from_js(
                "object",
                "数据结构嵌套超过 128 层（或存在环），拒绝序列化",
            ));
        }
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
            rquickjs::Type::String => Value::String(value.get::<String>()?),
            rquickjs::Type::Array => {
                let array = value.get::<Array>()?;
                let mut items = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    // 稀疏数组的洞读出来是 undefined：对应 stringify 的 [null]
                    let item = array.get::<JsValue>(i)?;
                    items.push(self.to_json(&item, depth + 1)?.unwrap_or(Value::Null));
                }
                Value::Array(items)
            }
            rquickjs::Type::BigInt => {
                return Err(rquickjs::Error::new_from_js(
                    "bigint",
                    "BigInt 无法序列化为 JSON",
                ));
            }
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
                    let item = object.get::<_, JsValue>(&key)?;
                    if let Some(json) = self.to_json(&item, depth + 1)? {
                        map.insert(key, json);
                    }
                }
                Value::Object(map)
            }
        }))
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
fn run_js(
    body: &str,
    input: &Value,
    nodes: &Value,
    tpl: &Value,
    timeout: Duration,
) -> Result<Value, EngineError> {
    let runtime = Runtime::new().map_err(|e| EngineError::Expr(e.to_string()))?;
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

        let result: JsValue = ctx.eval(program)?;
        let outcome = result.get::<Object>()?;
        let reader = JsReader::new(&ctx)?;
        if outcome.get::<_, bool>("ok")? {
            let value = outcome.get::<_, JsValue>("value")?;
            Ok(Ok(reader.to_json(&value, 0)?.unwrap_or(Value::Null)))
        } else {
            Ok(Err(outcome.get::<_, String>("error")?))
        }
    });
    match outcome.map_err(|e| EngineError::Expr(e.to_string()))? {
        Ok(value) => Ok(value),
        Err(message) => Err(EngineError::Expr(message)),
    }
}

/// 脚本节点：body 是一段带 `return` 的函数体，可用 `input` 与 `nodes`。
pub fn eval_body(
    body: &str,
    input: &Value,
    nodes: &Value,
    timeout: Duration,
) -> Result<Value, EngineError> {
    run_js(body, input, nodes, &Value::Null, timeout)
}

/// 条件节点：求值单个表达式。
pub fn eval_expr(
    expr: &str,
    input: &Value,
    nodes: &Value,
    timeout: Duration,
) -> Result<Value, EngineError> {
    let body = format!("    return ({expr});");
    run_js(&body, input, nodes, &Value::Null, timeout)
}

/// 展开字符串中的 `${expr}` 模板（用于 http_call 的 url/headers/body）。
pub fn expand_templates(
    tpl: &Value,
    input: &Value,
    nodes: &Value,
    timeout: Duration,
) -> Result<Value, EngineError> {
    let body = r#"    function __expand(v, input, nodes) {
      if (typeof v === 'string') {
        return v.replace(/\$\{([^}]+)\}/g, function (_, e) {
          var r = eval(e);
          return (typeof r === 'string') ? r : JSON.stringify(r);
        });
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
    run_js(body, input, nodes, tpl, timeout)
}

#[cfg(test)]
mod tests;
