//! Deliberately bounded JSON Schema subset for workflow API contracts.
use serde_json::Value;

pub fn check(schema: &Value) -> Result<(), String> {
    check_at(schema, 0)
}
fn check_at(s: &Value, depth: usize) -> Result<(), String> {
    if depth > 32 {
        return Err("schema depth exceeds 32".into());
    }
    if s.is_boolean() {
        return Ok(());
    }
    let object = s.as_object().ok_or("schema must be an object or boolean")?;
    for key in object.keys() {
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

pub fn validate(schema: &Value, value: &Value) -> Result<(), String> {
    check(schema)?;
    validate_at(schema, value, "$", 0)
}
fn validate_at(s: &Value, v: &Value, path: &str, depth: usize) -> Result<(), String> {
    if depth > 64 {
        return Err(format!("{path}: value depth exceeds 64"));
    }
    if s == &Value::Bool(false) {
        return Err(format!("{path}: value rejected by schema"));
    }
    if let Some(kind) = s["type"].as_str() {
        let valid = match kind {
            "object" => v.is_object(),
            "array" => v.is_array(),
            "string" => v.is_string(),
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
