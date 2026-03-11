mod file_info;

pub use file_info::{
    LocalFileInfo, build_local_file_info, compute_file_md5, extract_isbn_candidates,
    extract_md5_candidates, normalize_for_match, significant_terms,
};
