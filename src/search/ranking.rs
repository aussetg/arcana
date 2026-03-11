pub const BM25_WEIGHTS: [f64; 6] = [10.0, 8.0, 5.0, 1.5, 1.0, 0.5];

pub const KEYWORD_ORDER_BY: &str =
    "bm25_score ASC, COALESCE(r.score_base_rank, 0) DESC, r.rid DESC";

pub const EXACT_ORDER_BY: &str = "COALESCE(r.score_base_rank, 0) DESC, r.rid DESC";
