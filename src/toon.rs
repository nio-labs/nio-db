//! TOON v3.0 encoder for JSON values. Key folding and path expansion are disabled.
//! This server writes TOON projections; recovery uses the checksummed JSON journal.
use serde_json::Value;

pub fn encode(value: &Value) -> String {
    let mut out = String::new();
    match value {
        Value::Object(fields) => {
            for (key, value) in fields {
                field(&mut out, 0, key, value, "");
            }
        }
        Value::Array(values) => array(&mut out, 0, "", values, "", true),
        _ => line(&mut out, 0, &primitive(value)),
    }
    out
}

fn quoted(text: &str) -> String {
    // TOON uses five escapes; other control characters are emitted literally.
    format!(
        "\"{}\"",
        text.replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n")
            .replace('\r', "\\r")
            .replace('\t', "\\t")
    )
}

fn primitive(value: &Value) -> String {
    match value {
        Value::String(text) => quoted(text),
        _ => value.to_string(),
    }
}

fn line(out: &mut String, depth: usize, text: &str) {
    out.push_str(&"  ".repeat(depth));
    out.push_str(text);
    out.push('\n');
}

fn field(out: &mut String, depth: usize, key: &str, value: &Value, prefix: &str) {
    let key = quoted(key);
    match value {
        Value::Object(fields) => {
            line(out, depth, &format!("{prefix}{key}:"));
            // A first field carried on a hyphen line stands one depth deeper.
            let nested = depth + if prefix.is_empty() { 1 } else { 2 };
            for (key, value) in fields {
                field(out, nested, key, value, "");
            }
        }
        Value::Array(values) => array(out, depth, &key, values, prefix, true),
        _ => line(out, depth, &format!("{prefix}{key}: {}", primitive(value))),
    }
}

fn array(
    out: &mut String,
    depth: usize,
    key: &str,
    values: &[Value],
    prefix: &str,
    allow_table: bool,
) {
    let header = format!("{prefix}{key}[{}]", values.len());
    let child_depth = depth
        + if prefix == "- " && !key.is_empty() {
            2
        } else {
            1
        };
    if values.iter().all(|v| !v.is_array() && !v.is_object()) {
        let cells = values.iter().map(primitive).collect::<Vec<_>>().join(",");
        line(
            out,
            depth,
            &format!(
                "{header}:{}",
                if cells.is_empty() {
                    String::new()
                } else {
                    format!(" {cells}")
                }
            ),
        );
        return;
    }
    let fields = values.first().and_then(Value::as_object);
    if allow_table
        && fields.is_some_and(|fields| {
            !fields.is_empty()
                && values.iter().all(|v| {
                    v.as_object().is_some_and(|obj| {
                        obj.keys().eq(fields.keys())
                            && obj.values().all(|v| !v.is_array() && !v.is_object())
                    })
                })
        })
    {
        let fields = fields.unwrap();
        line(
            out,
            depth,
            &format!(
                "{header}{{{}}}:",
                fields
                    .keys()
                    .map(|k| quoted(k))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        );
        for value in values {
            line(
                out,
                child_depth,
                &fields
                    .keys()
                    .map(|k| primitive(&value[k]))
                    .collect::<Vec<_>>()
                    .join(","),
            );
        }
        return;
    }
    line(out, depth, &format!("{header}:"));
    for value in values {
        match value {
            Value::Object(fields) if fields.is_empty() => line(out, child_depth, "-"),
            Value::Object(fields) => {
                for (index, (key, value)) in fields.iter().enumerate() {
                    field(
                        out,
                        if index == 0 {
                            child_depth
                        } else {
                            child_depth + 1
                        },
                        key,
                        value,
                        if index == 0 { "- " } else { "" },
                    );
                }
            }
            Value::Array(values) => array(out, child_depth, "", values, "- ", false),
            _ => line(out, child_depth, &format!("- {}", primitive(value))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn uniform_rows_and_nested_mixed_values() {
        assert_eq!(
            encode(&json!({"users":[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]})),
            "\"users\"[2]{\"id\",\"name\"}:\n  1,\"Alice\"\n  2,\"Bob\"\n"
        );
        let value = json!({"items":[{"nested":{"x":"a\nb"},"rest":[]},null,[],{}]});
        assert_eq!(
            encode(&value),
            "\"items\"[4]:\n  - \"nested\":\n      \"x\": \"a\\nb\"\n    \"rest\"[0]:\n  - null\n  - [0]:\n  -\n"
        );
    }
}
