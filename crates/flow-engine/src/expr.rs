use std::time::{Duration, Instant};

use rquickjs::{Context, Runtime};
use serde::Deserialize;
use serde_json::Value;

use crate::error::EngineError;

#[derive(Deserialize)]
struct JsResult {
    ok: bool,
    #[serde(default)]
    value: Option<Value>,
    #[serde(default)]
    error: Option<String>,
}

/// 表达式求值统一入口：JS 沙箱，无 IO，带超时中断。
///
/// 走 JS 而不是自造 DSL：前端预览和引擎求值共用同一种语言。
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
    let input_json = input.to_string();
    let nodes_json = nodes.to_string();
    let tpl_json = tpl.to_string();

    let program = format!(
        r#"(function () {{
  var input = JSON.parse(__input_json);
  var nodes = JSON.parse(__nodes_json);
  var tpl = JSON.parse(__tpl_json);
  try {{
    var __v = (function () {{
{body}
    }})();
    return JSON.stringify({{ ok: true, value: (__v === undefined ? null : __v) }});
  }} catch (e) {{
    return JSON.stringify({{ ok: false, error: String((e && e.message) || e) }});
  }}
}})()"#
    );

    let raw: String = context
        .with(|ctx| -> Result<String, rquickjs::Error> {
            let globals = ctx.globals();
            globals.set("__input_json", input_json)?;
            globals.set("__nodes_json", nodes_json)?;
            globals.set("__tpl_json", tpl_json)?;
            ctx.eval::<String, _>(program)
        })
        .map_err(|e| EngineError::Expr(e.to_string()))?;

    let parsed: JsResult = serde_json::from_str(&raw)?;
    if parsed.ok {
        Ok(parsed.value.unwrap_or(Value::Null))
    } else {
        Err(EngineError::Expr(
            parsed.error.unwrap_or_else(|| "脚本执行失败".into()),
        ))
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
mod tests {
    use super::*;
    use serde_json::json;

    fn ctx() -> (Value, Value) {
        (
            json!({"order_id": "o-1", "amount": 12}),
            json!({"n2": {"status": 200}}),
        )
    }

    #[test]
    fn body_can_read_input_and_upstream_outputs() {
        let (input, nodes) = ctx();
        let out = eval_body(
            "return { id: input.order_id, upstream: nodes.n2.status, total: input.amount * 2 };",
            &input,
            &nodes,
            Duration::from_secs(2),
        )
        .unwrap();
        assert_eq!(out, json!({"id": "o-1", "upstream": 200, "total": 24}));
    }

    #[test]
    fn expr_returns_falsy_value_when_condition_not_met() {
        let (input, nodes) = ctx();
        let out = eval_expr("input.amount > 100", &input, &nodes, Duration::from_secs(2)).unwrap();
        assert_eq!(out, json!(false));
    }

    #[test]
    fn exceptions_surface_as_errors() {
        let (input, nodes) = ctx();
        let err = eval_body(
            "return nope.undefined_thing();",
            &input,
            &nodes,
            Duration::from_secs(2),
        )
        .unwrap_err();
        assert!(matches!(err, EngineError::Expr(_)), "{err}");
    }

    #[test]
    fn templates_expand_strings_and_embed_objects() {
        let (input, nodes) = ctx();
        let tpl = json!({
            "url": "https://api.example.com/orders/${input.order_id}",
            "body": {"raw": "${nodes.n2}", "keep": 1}
        });
        let out = expand_templates(&tpl, &input, &nodes, Duration::from_secs(2)).unwrap();
        assert_eq!(out["url"], json!("https://api.example.com/orders/o-1"));
        assert_eq!(out["body"]["raw"], json!("{\"status\":200}"));
        assert_eq!(out["body"]["keep"], json!(1));
    }

    #[test]
    fn runaway_script_is_interrupted_by_timeout() {
        let (input, nodes) = ctx();
        let err =
            eval_body("while (true) {}", &input, &nodes, Duration::from_millis(50)).unwrap_err();
        assert!(matches!(err, EngineError::Expr(_)), "{err}");
    }

    /// 沙箱边界的**行为**证据（DESIGN.md §10）：无 `std`/`os` 模块、无模块加载器。
    /// 升级 rquickjs 时这个测试自动重新验证——不靠读 build.rs 考古。
    /// 若某天升级后 `typeof std` 不再是 "undefined"，这里当场红。
    #[test]
    fn sandbox_has_no_std_os_or_module_loader() {
        let (input, nodes) = ctx();

        // quickjs-libc 的 std/os 模块不存在：typeof 必须是 "undefined"
        for module in ["std", "os", "quickjs"] {
            let out = eval_expr(
                &format!("typeof {module}"),
                &input,
                &nodes,
                Duration::from_secs(2),
            )
            .unwrap_or_else(|e| panic!("typeof {module} 不应报错：{e}"));
            assert_eq!(out, json!("undefined"), "{module} 模块必须不存在于沙箱");
        }

        // 无模块加载器：静态 import 语法直接被拒（eval/函数体内不允许，
        // 且无 loader 可供动态 import 解析——动态 import 只会得到永不 settle
        // 的 Promise，不可能加载到任何模块）
        let err = eval_body(
            "import 'whatever'; return 1;",
            &input,
            &nodes,
            Duration::from_secs(2),
        )
        .unwrap_err();
        assert!(matches!(err, EngineError::Expr(_)), "{err}");

        // 全局对象上也没有藏起来的口子：常见的逃逸名字全部 undefined
        for probe in [
            "globalThis.std",
            "globalThis.os",
            "globalThis.require",
            "globalThis.process",
            "globalThis.fetch",
            "globalThis.XMLHttpRequest",
        ] {
            let out = eval_expr(
                &format!("typeof {probe}"),
                &input,
                &nodes,
                Duration::from_secs(2),
            )
            .unwrap();
            assert_eq!(out, json!("undefined"), "{probe} 必须不可用");
        }
    }
}
