use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use super::ExpansionCandidate;

pub trait ExpansionProvider {
    fn provider_name(&self) -> String;
    fn generate(&self, normalized_query: &str, max_candidates: usize) -> Result<ProviderOutput>;
}

#[derive(Debug, Clone)]
pub struct ProviderOutput {
    pub raw_response: String,
    pub candidates: Vec<ExpansionCandidate>,
}

#[derive(Debug, Clone)]
pub struct LlamaCliProvider {
    command: String,
    model_path: PathBuf,
    timeout_secs: u64,
}

impl LlamaCliProvider {
    pub fn new(command: String, model_path: PathBuf, timeout_secs: u64) -> Self {
        Self {
            command,
            model_path,
            timeout_secs,
        }
    }
}

impl ExpansionProvider for LlamaCliProvider {
    fn provider_name(&self) -> String {
        format!("{}:{}", self.command, self.model_path.display())
    }

    fn generate(&self, normalized_query: &str, max_candidates: usize) -> Result<ProviderOutput> {
        let model_path = expand_tilde_path(&self.model_path);
        if !model_path.exists() {
            bail!("expansion model not found: {}", model_path.display());
        }

        let prompt = build_prompt(normalized_query, max_candidates);

        let mut child = Command::new(&self.command)
            .arg("-m")
            .arg(&model_path)
            .arg("-p")
            .arg(prompt)
            .arg("-n")
            .arg("256")
            .arg("--temp")
            .arg("0")
            .arg("--seed")
            .arg("42")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("failed to start {}", self.command))?;

        let started = Instant::now();
        loop {
            if child.try_wait()?.is_some() {
                let output = child.wait_with_output()?;
                if !output.status.success() {
                    bail!(
                        "{} exited with status {}: {}",
                        self.command,
                        output.status,
                        String::from_utf8_lossy(&output.stderr).trim()
                    );
                }

                let stdout = String::from_utf8(output.stdout)
                    .context("expansion provider output was not valid UTF-8")?;
                let parsed = parse_provider_json(&stdout)?;

                return Ok(ProviderOutput {
                    raw_response: stdout,
                    candidates: parsed.expansions,
                });
            }

            if started.elapsed() >= Duration::from_secs(self.timeout_secs) {
                let _ = child.kill();
                let _ = child.wait_with_output();
                bail!("{} timed out after {}s", self.command, self.timeout_secs);
            }

            thread::sleep(Duration::from_millis(25));
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProviderJson {
    expansions: Vec<ExpansionCandidate>,
}

fn build_prompt(normalized_query: &str, max_candidates: usize) -> String {
    format!(
        "You generate lexical search query variants for a local metadata search engine.\n\
Return JSON only. No markdown. No prose.\n\
Schema: {{\"expansions\":[{{\"text\":\"...\",\"kind\":\"...\",\"confidence\":0.0}}]}}\n\
Rules:\n\
- Output at most {max_candidates} expansions.\n\
- Each expansion must be a full alternative query for the whole input query.\n\
- Prefer close lexical variants likely to appear literally in titles, subjects, descriptions, or publisher metadata.\n\
- Do not output broad topic neighbors or brainstorming terms.\n\
- Keep intent tight.\n\
- If no safe expansion exists, return {{\"expansions\":[]}}.\n\
Input query: {normalized_query:?}\n"
    )
}

fn parse_provider_json(raw: &str) -> Result<ProviderJson> {
    if let Ok(parsed) = serde_json::from_str::<ProviderJson>(raw.trim()) {
        return Ok(parsed);
    }

    let trimmed = raw.trim();
    let start = trimmed
        .find('{')
        .context("provider returned no JSON object")?;
    let end = trimmed
        .rfind('}')
        .context("provider returned no JSON object")?;
    let object = &trimmed[start..=end];
    Ok(serde_json::from_str(object)?)
}

fn expand_tilde_path(path: &Path) -> PathBuf {
    let path_text = path.to_string_lossy();
    if !path_text.starts_with("~/") {
        return path.to_path_buf();
    }

    match std::env::var_os("HOME") {
        Some(home) => PathBuf::from(home).join(&path_text[2..]),
        None => path.to_path_buf(),
    }
}
