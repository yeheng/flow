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

/// 原生对象注入必须与 JSON.parse 的语义一致："__proto__" 是自有数据键，
/// 不是原型链入口（Object.set 走 setter 通道会在这里分叉）。
#[test]
fn injected_objects_keep_proto_as_plain_data_key() {
    let (input, _) = ctx();
    let out = eval_body(
        "return { seen: Object.prototype.hasOwnProperty.call(nodes, '__proto__'), \
         value: nodes['__proto__'] };",
        &input,
        &json!({"__proto__": {"polluted": true}}),
        Duration::from_secs(2),
    )
    .unwrap();
    assert_eq!(out["seen"], json!(true));
    assert_eq!(out["value"], json!({"polluted": true}));
}

/// 返回侧同样对齐 JSON.stringify：整数值浮点回填整数（2.0 → 2），
/// Date 按 toJSON 约定出 ISO 字符串。
#[test]
fn result_conversion_matches_json_stringify() {
    let (input, nodes) = ctx();
    let out = eval_body(
        "return { half: input.amount / 4, whole: (input.amount / 4) * 2, \
         when: new Date(0), fn: function () {}, nested: { drop: undefined, keep: 1 } };",
        &input,
        &nodes,
        Duration::from_secs(2),
    )
    .unwrap();
    assert_eq!(out["half"], json!(3));
    assert_eq!(out["whole"], json!(6));
    assert_eq!(out["when"], json!("1970-01-01T00:00:00.000Z"));
    // function/undefined 与 stringify 一致：从对象里消失
    assert!(out.get("fn").is_none());
    assert!(out["nested"].get("drop").is_none());
    assert_eq!(out["nested"]["keep"], json!(1));
}

/// BigInt 与 stringify 同样拒绝（报错而非静默丢值）。
#[test]
fn bigint_result_is_rejected_like_stringify() {
    let (input, nodes) = ctx();
    let err = eval_body("return 1n + 2n;", &input, &nodes, Duration::from_secs(2)).unwrap_err();
    assert!(matches!(err, EngineError::Expr(_)), "{err}");
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
