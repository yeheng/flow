//! Deliberately bounded JSON Schema subset for workflow API contracts.
//!
//! 本模块负责工作流输入/输出 schema 的**受限子集**校验，分两步：
//! `check` 先判定 schema 自身是否只用被允许的关键字，`validate` 再按它
//! 校验取值。
//!
//! 它守护的核心不变量是「未知关键字一律拒绝，而不是忽略后放过」——
//! 用户以为生效的约束若被静默丢弃，契约与实现就分叉了，而分叉只在
//! 某条异常输入流过时才暴露。
//!
//! 不这样做的代价：一份带 `oneOf`/`$ref` 的 schema 看起来合法，实际却
//! 「什么都没校验」，非法输入照样通过，问题会推迟到更远的地方才炸。
use serde_json::Value;

/// 校验一份 schema 是否落在受支持的子集内。
///
/// 只看 schema 本身，不看任何取值：保存期用它拒绝越界的 schema，
/// 运行期 `validate` 也会先跑一遍同样的检查，保证两条路径口径一致。
///
/// # Errors
/// 返回描述第一个越界之处的字符串（未知关键字、非法类型、深度超限等）。
pub fn check(schema: &Value) -> Result<(), String> {
    check_at(schema, 0)
}

/// 递归校验 schema 子集，depth 是当前嵌套层数。
///
/// 深度上限 32 是防栈溢出的硬闸门：schema 由用户提供，手工构造的深链
/// 在无上限的递归里会把 Rust 栈打爆（进程级崩溃，不是可捕获错误）。
fn check_at(s: &Value, depth: usize) -> Result<(), String> {
    if depth > 32 {
        return Err("schema depth exceeds 32".into());
    }
    // draft-07 允许 boolean schema：`true` 恒通过、`false` 恒拒绝
    if s.is_boolean() {
        return Ok(());
    }
    let object = s.as_object().ok_or("schema must be an object or boolean")?;
    for key in object.keys() {
        // 白名单逐字列出：新增关键字必须同时改这里，否则用户写了会
        // 得到「unsupported schema keyword」而不是被悄悄忽略。
        if !matches!(
            key.as_str(),
            "type"
                | "properties"
                | "required"
                | "additionalProperties"
                | "items"
                | "enum"
                | "minLength"
                | "maxLength"
                | "minimum"
                | "maximum"
                | "minItems"
                | "maxItems"
                | "title"
                | "description"
                | "default"
        ) {
            return Err(format!("unsupported schema keyword: {key}"));
        }
    }
    if let Some(t) = s.get("type") {
        if !matches!(
            t.as_str(),
            Some("object" | "array" | "string" | "number" | "integer" | "boolean" | "null")
        ) {
            return Err("unsupported schema type".into());
        }
    }
    if let Some(properties) = s.get("properties") {
        for v in properties
            .as_object()
            .ok_or("schema properties must be an object")?
            .values()
        {
            check_at(v, depth + 1)?;
        }
    }
    for name in ["items", "additionalProperties"] {
        if let Some(v) = s.get(name) {
            check_at(v, depth + 1)?;
        }
    }
    if let Some(required) = s.get("required") {
        let a = required
            .as_array()
            .ok_or("schema required must be an array")?;
        if a.iter().any(|v| !v.is_string()) {
            return Err("schema required entries must be strings".into());
        }
    }
    if let Some(e) = s.get("enum") {
        // 空 enum 恒不匹配任何取值：与其让它变成「所有值都非法」的
        // 隐式陷阱，不如在保存期就拒掉。
        if e.as_array().is_none_or(|a| a.is_empty()) {
            return Err("schema enum must be a nonempty array".into());
        }
    }
    for name in ["minLength", "maxLength", "minItems", "maxItems"] {
        if s.get(name).is_some_and(|v| v.as_u64().is_none()) {
            return Err(format!("schema {name} must be a nonnegative integer"));
        }
    }
    for name in ["minimum", "maximum"] {
        if s.get(name).is_some_and(|v| !v.is_number()) {
            return Err(format!("schema {name} must be numeric"));
        }
    }
    Ok(())
}

/// 按 schema 校验一个取值，先检查 schema 自身合法性再校验取值。
///
/// 输入输出合同共用这一条路径：根流程与子流程的输入都在建 run 前过它，
/// 输出不合约时 run 失败（见 EXECUTION.md 的输入输出合同）。
///
/// # Errors
/// 返回第一条不满足的约束，路径用 `$` / `.key` / `[i]` 标注，便于定位。
pub fn validate(schema: &Value, value: &Value) -> Result<(), String> {
    check(schema)?;
    validate_at(schema, value, "$", 0)
}

/// 递归校验取值；path 是给人看的错误定位路径，depth 是嵌套层数。
///
/// 深度上限 64 同时挡住「schema 嵌套深」与「取值嵌套深」两种输入，
/// 避免递归把栈打爆。
fn validate_at(s: &Value, v: &Value, path: &str, depth: usize) -> Result<(), String> {
    if depth > 64 {
        return Err(format!("{path}: value depth exceeds 64"));
    }
    // boolean schema `false` 拒绝一切取值（含 `additionalProperties: false`）
    if s == &Value::Bool(false) {
        return Err(format!("{path}: value rejected by schema"));
    }
    if let Some(kind) = s["type"].as_str() {
        let valid = match kind {
            "object" => v.is_object(),
            "array" => v.is_array(),
            "string" => v.is_string(),
            // integer 必须是真整数：2.0 这类浮点形态不算（JSON 数字语义）
            "integer" => v.as_i64().is_some() || v.as_u64().is_some(),
            "number" => v.is_number(),
            "boolean" => v.is_boolean(),
            "null" => v.is_null(),
            _ => false,
        };
        if !valid {
            return Err(format!("{path}: expected {kind}"));
        }
    }
    if let Some(e) = s["enum"].as_array() {
        if !e.contains(v) {
            return Err(format!("{path}: value is outside enum"));
        }
    }
    if let Some(object) = v.as_object() {
        if let Some(required) = s["required"].as_array() {
            for key in required {
                let k = key.as_str().unwrap_or_default();
                if !object.contains_key(k) {
                    return Err(format!("{path}.{k}: required property missing"));
                }
            }
        }
        for (key, value) in object {
            if let Some(property) = s["properties"].get(key) {
                validate_at(property, value, &format!("{path}.{key}"), depth + 1)?;
            } else if let Some(additional) = s.get("additionalProperties") {
                // additionalProperties 复用同一递归：`false` 会在这里拒掉
                // schema 未声明的多余键，对象形态则按子 schema 校验。
                validate_at(additional, value, &format!("{path}.{key}"), depth + 1)?;
            }
        }
    }
    if let Some(array) = v.as_array() {
        bounds(s, array.len() as u64, "minItems", "maxItems", path)?;
        if let Some(items) = s.get("items") {
            for (i, v) in array.iter().enumerate() {
                validate_at(items, v, &format!("{path}[{i}]"), depth + 1)?;
            }
        }
    }
    if let Some(text) = v.as_str() {
        bounds(
            s,
            text.chars().count() as u64,
            "minLength",
            "maxLength",
            path,
        )?;
    }
    if v.is_number()
        && (s.get("minimum").is_some_and(|bound| less(v, bound))
            || s.get("maximum").is_some_and(|bound| less(bound, v)))
    {
        return Err(format!("{path}: number outside bounds"));
    }
    Ok(())
}
/// 比较两个数值大小，整数优先按 i128 精确比较。
///
/// 不能一律转 f64：超过 2^53 的整数经 f64 会静默舍入，使 `minimum`
/// 边界判断失真（9007199254740993 与 9007199254740992 会被判成相等）。
fn less(a: &Value, b: &Value) -> bool {
    let integer = |n: &Value| {
        n.as_i64()
            .map(i128::from)
            .or_else(|| n.as_u64().map(i128::from))
    };
    match (integer(a), integer(b)) {
        (Some(a), Some(b)) => a < b,
        _ => a.as_f64().zip(b.as_f64()).is_some_and(|(a, b)| a < b),
    }
}

/// 长度类边界检查（minLength/maxLength/minItems/maxItems）。
///
/// 长度一律按**字符数**而非字节数计，与 JSON Schema 的定义一致；
/// 字节数会让多字节文本在同一份 schema 下得到不同判定。
fn bounds(s: &Value, n: u64, min: &str, max: &str, path: &str) -> Result<(), String> {
    if s[min].as_u64().is_some_and(|b| n < b) || s[max].as_u64().is_some_and(|b| n > b) {
        Err(format!("{path}: size outside bounds"))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    /// 业务合同的核心形状：缺必填、类型错、越界、多余键都要被拒。
    /// 顺带钉住「未知关键字拒绝」——`oneOf` 不在子集内。
    #[test]
    fn business_contract_rejects_missing_wrong_and_extra_values() {
        let schema = json!({"type":"object","required":["quantity"],"properties":{"quantity":{"type":"integer","minimum":1}},"additionalProperties":false});
        assert!(validate(&schema, &json!({"quantity":2})).is_ok());
        for value in [
            json!({}),
            json!({"quantity":"2"}),
            json!({"quantity":0}),
            json!({"quantity":2,"admin":true}),
        ] {
            assert!(validate(&schema, &value).is_err());
        }
        assert!(check(&json!({"oneOf":[]})).is_err());
    }
    /// 整数边界不得经 f64 舍入：2^53 附近的上下界必须被精确区分。
    #[test]
    fn integer_bounds_do_not_round_through_f64() {
        assert!(validate(
            &json!({"minimum":9_007_199_254_740_993u64}),
            &json!(9_007_199_254_740_992u64)
        )
        .is_err());
        assert!(validate(
            &json!({"maximum":9_007_199_254_740_992u64}),
            &json!(9_007_199_254_740_993u64)
        )
        .is_err());
    }
}
