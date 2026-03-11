mod execute;
pub mod expand;
mod orchestrate;
pub mod query;
pub mod ranking;

pub use execute::{has_fts_rows, search_exact, search_keyword};
pub use orchestrate::{KeywordSearchOutput, search_keyword_with_expansion};

use execute::count_keyword_matches;

#[cfg(test)]
mod tests;
