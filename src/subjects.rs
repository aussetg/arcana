use std::collections::HashSet;

use serde_json::Value;

use crate::normalize;

const SUBJECT_KEYS: &[&str] = &[
    "openlib_subject",
    "zlib_category_name",
    "rgb_subject",
    "sciencereading_category",
    "edsebk_subject",
    "lgrsnf_topic",
    "nexusstc_tag",
];

pub fn synthesize(classifications: Option<&Value>) -> Option<String> {
    let Some(classifications) = classifications.and_then(Value::as_object) else {
        return None;
    };

    let mut seen = HashSet::new();
    let mut subjects = Vec::new();

    for key in SUBJECT_KEYS {
        for subject in normalize::cleaned_texts(classifications.get(*key)) {
            if !looks_human_meaningful(&subject) {
                continue;
            }

            let normalized = subject.to_ascii_lowercase();
            if seen.insert(normalized) {
                subjects.push(subject);
            }
        }
    }

    normalize::join_parts(subjects, " ; ")
}

fn looks_human_meaningful(value: &str) -> bool {
    if value.len() > 300 || !value.chars().any(|ch| ch.is_alphabetic()) {
        return false;
    }

    let lower = value.to_ascii_lowercase();

    if lower.contains("://")
        || lower.contains(".torrent")
        || value.starts_with('/')
        || value.starts_with("./")
        || value.starts_with("../")
        || value.starts_with("~/")
        || value.contains('\\')
    {
        return false;
    }

    !looks_like_hash(value)
}

fn looks_like_hash(value: &str) -> bool {
    value.len() >= 16 && value.chars().all(|ch| ch.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::synthesize;

    #[test]
    fn synthesizes_subjects_in_readable_order() {
        let classifications = json!({
            "openlib_subject": [" Machine learning ", "NLP"],
            "rgb_subject": ["machine learning", "Artificial Intelligence"],
            "zlib_category_name": ["/srv/path/to/file", "abcdef1234567890", "Computer Science"]
        });

        assert_eq!(
            synthesize(Some(&classifications)),
            Some("Machine learning ; NLP ; Computer Science ; Artificial Intelligence".into())
        );
    }
}
