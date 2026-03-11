mod flatten;
mod reader;
mod shards;

pub use reader::{FileStats, parse_ndjson_line, stream_records};
pub use shards::discover_input_shards;

#[cfg(test)]
mod tests;
