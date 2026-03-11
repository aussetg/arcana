use serde_json::Value;

pub fn clean_text(input: &str) -> Option<String> {
    let collapsed = input.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.is_empty() {
        None
    } else {
        Some(collapsed)
    }
}

pub fn trim_to_option(input: &str) -> Option<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

pub fn raw_strings(value: Option<&Value>) -> Vec<String> {
    let mut out = Vec::new();
    collect_raw_strings(value, &mut out);
    out
}

pub fn cleaned_texts(value: Option<&Value>) -> Vec<String> {
    raw_strings(value)
        .into_iter()
        .filter_map(|value| clean_text(&value))
        .collect()
}

pub fn trimmed_strings(value: Option<&Value>) -> Vec<String> {
    raw_strings(value)
        .into_iter()
        .filter_map(|value| trim_to_option(&value))
        .collect()
}

pub fn first_clean_text(value: Option<&Value>) -> Option<String> {
    cleaned_texts(value).into_iter().next()
}

pub fn first_trimmed_string(value: Option<&Value>) -> Option<String> {
    trimmed_strings(value).into_iter().next()
}

pub fn first_present<I>(values: I) -> Option<String>
where
    I: IntoIterator<Item = Option<String>>,
{
    values.into_iter().flatten().next()
}

pub fn join_parts(parts: Vec<String>, separator: &str) -> Option<String> {
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(separator))
    }
}

pub fn parse_i64(value: Option<&Value>) -> Option<i64> {
    match value? {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => text.trim().parse().ok(),
        _ => None,
    }
}

pub fn parse_year(value: Option<&Value>) -> Option<i32> {
    let text = first_trimmed_string(value)?;

    if text.len() != 4 || !text.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }

    text.parse().ok()
}

pub fn parse_bool_flag(value: Option<&Value>) -> Option<i64> {
    match value? {
        Value::Bool(flag) => Some(i64::from(*flag)),
        Value::Number(number) => number.as_i64().map(|n| i64::from(n != 0)),
        Value::String(text) => match text.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" => Some(1),
            "0" | "false" | "no" => Some(0),
            _ => None,
        },
        _ => None,
    }
}

fn collect_raw_strings(value: Option<&Value>, out: &mut Vec<String>) {
    let Some(value) = value else {
        return;
    };

    match value {
        Value::Null => {}
        Value::String(text) => out.push(text.clone()),
        Value::Number(number) => out.push(number.to_string()),
        Value::Bool(flag) => out.push(flag.to_string()),
        Value::Array(values) => {
            for value in values {
                collect_raw_strings(Some(value), out);
            }
        }
        Value::Object(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        clean_text, first_clean_text, join_parts, parse_bool_flag, parse_i64, parse_year,
        trimmed_strings,
    };

    #[test]
    fn cleans_whitespace() {
        assert_eq!(
            clean_text("  hello\n   world  "),
            Some("hello world".into())
        );
    }

    #[test]
    fn extracts_first_clean_text_from_arrays() {
        assert_eq!(
            first_clean_text(Some(&json!(["  ", " first ", "second"]))),
            Some("first".into())
        );
    }

    #[test]
    fn parses_scalar_types() {
        assert_eq!(parse_i64(Some(&json!("42"))), Some(42));
        assert_eq!(parse_year(Some(&json!("2024"))), Some(2024));
        assert_eq!(parse_year(Some(&json!("2024?"))), None);
        assert_eq!(parse_bool_flag(Some(&json!(true))), Some(1));
        assert_eq!(parse_bool_flag(Some(&json!("no"))), Some(0));
    }

    #[test]
    fn trims_string_arrays() {
        assert_eq!(
            trimmed_strings(Some(&json!(["  abc  ", "", " def "]))),
            vec!["abc".to_string(), "def".to_string()]
        );
    }

    #[test]
    fn joins_non_empty_parts() {
        assert_eq!(
            join_parts(vec!["one".into(), "two".into()], " ; "),
            Some("one ; two".into())
        );
        assert_eq!(join_parts(Vec::new(), " ; "), None);
    }
}
