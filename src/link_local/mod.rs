mod file_info;
mod matcher;
mod scan;

pub use file_info::{
    LocalFileInfo, build_local_file_info, compute_file_md5, extract_isbn_candidates,
    extract_md5_candidates, normalize_for_match, significant_terms,
};
pub use matcher::{
    AmbiguousMatch, CandidateRecord, LocalMatch, MatchEvidence, MatchMethod, MatchSearchOutcome,
    find_local_match,
};
pub use scan::discover_local_files;
