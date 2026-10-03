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
        &NodeLogger::disabled(),
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
        &NodeLogger::disabled(),
    )
    .unwrap_err();
    assert!(matches!(err, EngineError::Expr(_)), "{err}");
}

#[test]
fn bounded_script_memory_bomb_fails_instead_of_oom() {
    // 生产 driver 路径已切到 _bounded 变体：分配炸弹必须在 JS 堆限内
    // 失败，而不是吃光宿主进程内存（32 MiB 堆限，翻倍 25 次即触顶）。
    let (input, nodes) = ctx();
    let err = eval_body_bounded(
        "var s = 'x'; while (true) { s += s; } return s;",
        &input,
        &nodes,
        Duration::from_secs(10),
        &NodeLogger::disabled(),
    )
    .unwrap_err();
    assert!(matches!(err, EngineError::Expr(_)), "{err}");
}

#[test]
fn templates_expand_strings_and_embed_objects() {
    let (input, nodes) = ctx();
    let tpl = json!({
        "url": "https://api.example.com/orders/${input.order_id}",
        "token": "${input.order_id}",
        "nested": 15,
        "body": {"raw": "${nodes.n2}", "keep": 1}
    });
    let out = expand_templates(&tpl, &input, &nodes, Duration::from_secs(2)).unwrap();
    // 插值模板：字符串语义，逐字不变
    assert_eq!(out["url"], json!("https://api.example.com/orders/o-1"));
    // 整值字符串模板：两条规则下同结果
    assert_eq!(out["token"], json!("o-1"));
    // 无模板的标量原样
    assert_eq!(out["nested"], json!(15));
    // 整值模板 + 非字符串：按原类型替换（数字/对象不再被 stringify 成字符串）
    assert_eq!(out["body"]["raw"], json!({"status":200}));
    assert_eq!(out["body"]["keep"], json!(1));
}

/// 整值模板的类型保留规则：数字/布尔/null 原样穿透，`undefined` 归一 null。
/// input_mapping 之类的结构化传参依赖这条（否则数字进子 run 全变字符串）。
#[test]
fn whole_value_templates_preserve_json_types() {
    let (input, nodes) = ctx();
    let tpl = json!({
        "n": "${input.amount}",
        "nul": "${input.nope}",
        "flag": "${nodes.n2.status === 200}",
        "arr": "${[1, 2]}",
        "mixed": "x-${input.order_id}"
    });
    let out = expand_templates(&tpl, &input, &nodes, Duration::from_secs(2)).unwrap();
    assert_eq!(out["n"], json!(12));
    assert_eq!(out["nul"], json!(null), "undefined 应归一为 null");
    assert_eq!(out["flag"], json!(true));
    assert_eq!(out["arr"], json!([1, 2]));
    assert_eq!(out["mixed"], json!("x-o-1"), "嵌在文本里的模板仍是插值");
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
        &NodeLogger::disabled(),
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
        &NodeLogger::disabled(),
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

/// BigInt 结果精确转回 JSON 整数：脚本内 BigInt 算术不再被 stringify 拒绝，
/// 也不再静默丢值（DESIGN §10 大整数边界）。
#[test]
fn bigint_results_convert_exactly() {
    let (input, nodes) = ctx();
    let out = eval_body(
        "return 1n + 2n;",
        &input,
        &nodes,
        Duration::from_secs(2),
        &NodeLogger::disabled(),
    )
    .unwrap();
    assert_eq!(out, json!(3));
}

/// 大整数经 input/nodes 进出沙箱精确无损：|v| > 2^53 的整数以 BigInt
/// 注入，出口按 i64/u64 精确还原——雪花 ID 不再被静默舍入。
#[test]
fn big_integers_round_trip_exactly() {
    let input = json!({
        "snowflake": 9007199254740993i64,          // 2^53+1：曾被舍入为 2^53
        "neg": -9007199254740993i64,
        "i64_min": i64::MIN,
        "u64_max": u64::MAX,
        "small": 9007199254740992i64,             // 2^53：仍走 Number，精确
        "float": 1.5
    });
    let nodes = json!({"n": {"id": 18446744073709551615u64}});
    let out = eval_body(
        "return { a: input.snowflake, b: input.neg, c: input.i64_min, \
                 d: input.u64_max, e: input.small, f: input.float, g: nodes.n.id };",
        &input,
        &nodes,
        Duration::from_secs(2),
        &NodeLogger::disabled(),
    )
    .unwrap();
    assert_eq!(out["a"], json!(9007199254740993i64));
    assert_eq!(out["b"], json!(-9007199254740993i64));
    assert_eq!(out["c"], json!(i64::MIN));
    assert_eq!(out["d"], json!(u64::MAX));
    assert_eq!(out["e"], json!(9007199254740992i64));
    assert_eq!(out["f"], json!(1.5));
    assert_eq!(out["g"], json!(u64::MAX));
}

/// BigInt 与 Number 混算响亮失败（TypeError），不再是静默错值。
/// 比较请用 n 后缀字面量（`9007199254740993n`）。  
#[test]
fn big_int_mixing_number_fails_loudly() {
    let input = json!({"id": 9007199254740993i64});
    let err = eval_body(
        "return input.id + 1;",
        &input,
        &Value::Null,
        Duration::from_secs(2),
        &NodeLogger::disabled(),
    )
    .unwrap_err();
    let EngineError::Expr(msg) = &err else {
        panic!("应为 Expr 错误：{err}")
    };
    assert!(
        msg.to_lowercase().contains("bigint"),
        "错误应指向 BigInt 混算：{msg}"
    );

    // 比较必须用 BigInt 字面量：与 Number 字面量（已舍入）永不相等
    let out = eval_expr(
        "input.id === 9007199254740993n",
        &input,
        &Value::Null,
        Duration::from_secs(2),
    )
    .unwrap();
    assert_eq!(out, json!(true));
}

/// 超出 JSON 整数域（i64/u64）的 BigInt 结果响亮报错，绝不静默截断。
#[test]
fn oversized_bigint_result_is_rejected() {
    let (input, nodes) = ctx();
    let err = eval_body(
        "return 2n ** 64n;",
        &input,
        &nodes,
        Duration::from_secs(2),
        &NodeLogger::disabled(),
    )
    .unwrap_err();
    assert!(matches!(err, EngineError::Expr(_)), "{err}");
}

/// 模板展开的大整数语义：整值模板按原类型（精确 JSON 整数）穿透，
/// 插值模板对 BigInt 用 toString 拿精确十进制（雪花 ID 拼 URL）。  
#[test]
fn templates_handle_big_integers_exactly() {
    let input = json!({"id": 9007199254740993i64, "big": u64::MAX});
    let tpl = json!({
        "whole": "${input.id}",
        "url": "https://api.example.com/users/${input.id}",
        "both": "${input.id}/${input.big}"
    });
    let out = expand_templates(&tpl, &input, &Value::Null, Duration::from_secs(2)).unwrap();
    assert_eq!(out["whole"], json!(9007199254740993i64));
    assert_eq!(
        out["url"],
        json!("https://api.example.com/users/9007199254740993")
    );
    assert_eq!(out["both"], json!("9007199254740993/18446744073709551615"));
}

#[test]
fn runaway_script_is_interrupted_by_timeout() {
    let (input, nodes) = ctx();
    let err = eval_body(
        "while (true) {}",
        &input,
        &nodes,
        Duration::from_millis(50),
        &NodeLogger::disabled(),
    )
    .unwrap_err();
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
        &NodeLogger::disabled(),
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
