use crate::tmdb::client::TmdbSearchResult;
use strsim::normalized_levenshtein;

#[derive(Debug, Clone)]
pub struct ScoredCandidate {
    pub tmdb_result: TmdbSearchResult,
    pub score: f64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum MatchDecision {
    AutoConfirm,
    Pending,
    Failed,
}

/// 给一对 (query, candidate) 标题/年份打分，0.0~1.0。
///
/// Scoring breakdown:
/// - Title similarity (weight 0.6): query_title 跟 cand_title / cand_alt_title
///   都做 normalized Levenshtein，取 max
/// - Year match (weight 0.3): exact = 0.3, +/-1 = 0.25, else 0.0
///   Off-by-one 保持高分有意为之——节展首映 vs 公映年份经常差 1
///   （Beau Travail 1999 Venice / 2000 France；Inu-oh 2021 Venice / 2022 wide）
/// - Popularity boost (weight 0.1): (popularity / 100) cap 1.0
///
/// 这个纯函数被两条路径调用：
/// - `score_candidates`: 把 TMDB 搜索结果作为 candidate 跟 parsed 目录名打分
/// - `api::movies::locate_movie`: 反过来，把电影元信息作为 query 跟未绑定目录的
///   parsed 名字打分
pub fn score_title_year(
    query_title: &str,
    query_alt_title: Option<&str>,
    query_year: Option<u16>,
    cand_title: &str,
    cand_alt_title: Option<&str>,
    cand_year: Option<u16>,
    cand_popularity: Option<f64>,
) -> f64 {
    let q_primary = query_title.to_lowercase();
    let q_alt = query_alt_title.map(|s| s.to_lowercase());
    let c_primary = cand_title.to_lowercase();
    let c_alt = cand_alt_title.map(|s| s.to_lowercase());

    // 全部 (query_*, cand_*) 组合取最高相似度。
    let mut title_sim = normalized_levenshtein(&q_primary, &c_primary);
    if let Some(ref a) = c_alt {
        title_sim = title_sim.max(normalized_levenshtein(&q_primary, a));
    }
    if let Some(ref qa) = q_alt {
        title_sim = title_sim.max(normalized_levenshtein(qa, &c_primary));
        if let Some(ref a) = c_alt {
            title_sim = title_sim.max(normalized_levenshtein(qa, a));
        }
    }
    let title_score = title_sim * 0.6;

    let year_score = match (query_year, cand_year) {
        (Some(qy), Some(cy)) => {
            if qy == cy {
                0.3
            } else if (qy as i32 - cy as i32).abs() == 1 {
                0.25
            } else {
                0.0
            }
        }
        _ => 0.0,
    };

    let popularity_score = cand_popularity.map(|p| (p / 100.0).min(1.0) * 0.1).unwrap_or(0.0);

    title_score + year_score + popularity_score
}

/// Score TMDB search results against a parsed title and optional year.
/// 薄包装；语义见 [`score_title_year`]。
pub fn score_candidates(
    parsed_title: &str,
    parsed_year: Option<u16>,
    candidates: Vec<TmdbSearchResult>,
) -> Vec<ScoredCandidate> {
    let mut scored: Vec<ScoredCandidate> = candidates
        .into_iter()
        .map(|candidate| {
            let cand_year = release_year(&candidate);
            let score = score_title_year(
                parsed_title,
                None,
                parsed_year,
                &candidate.title,
                candidate.original_title.as_deref(),
                cand_year,
                candidate.popularity,
            );
            ScoredCandidate { tmdb_result: candidate, score }
        })
        .collect();

    scored.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
    scored
}

fn release_year(candidate: &TmdbSearchResult) -> Option<u16> {
    candidate
        .release_date
        .as_ref()
        .and_then(|d| d.get(0..4))
        .and_then(|y| y.parse::<u16>().ok())
}

pub fn decide_match(top_score: f64, threshold: f64) -> MatchDecision {
    if top_score >= threshold {
        MatchDecision::AutoConfirm
    } else if top_score >= 0.5 {
        MatchDecision::Pending
    } else {
        MatchDecision::Failed
    }
}

/// Conflict guard: when sidecar evidence pulls in candidates from multiple
/// distinct films (different tmdb_id) that score close to each other, we
/// can't tell which one is real — auto-confirming would coin-flip. Returns
/// true if top-1 is "safely" ahead of the next distinct tmdb_id.
///
/// Returns true when:
/// - there's only one distinct tmdb_id, OR
/// - top-1 outranks the next distinct tmdb_id by `min_gap` or more.
///
/// Caller passes `scored` already sorted desc by score.
pub fn is_unambiguous_winner(scored: &[ScoredCandidate], min_gap: f64) -> bool {
    if scored.is_empty() {
        return false;
    }
    let top_id = scored[0].tmdb_result.id;
    let top_score = scored[0].score;
    for c in &scored[1..] {
        if c.tmdb_result.id == top_id {
            continue;
        }
        return (top_score - c.score) >= min_gap;
    }
    // Only one distinct tmdb_id in candidates.
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_scored(id: i64, score: f64) -> ScoredCandidate {
        ScoredCandidate {
            tmdb_result: TmdbSearchResult {
                id,
                title: format!("t{}", id),
                original_title: None,
                release_date: None,
                overview: None,
                poster_path: None,
                vote_average: None,
                vote_count: None,
                popularity: None,
                genre_ids: None,
                original_language: None,
            },
            score,
        }
    }

    #[test]
    fn unambiguous_when_only_one_distinct_id() {
        let scored = vec![make_scored(1, 0.9), make_scored(1, 0.8)];
        assert!(is_unambiguous_winner(&scored, 0.05));
    }

    #[test]
    fn unambiguous_when_gap_exceeds_threshold() {
        let scored = vec![make_scored(1, 0.9), make_scored(2, 0.8)];
        assert!(is_unambiguous_winner(&scored, 0.05));
    }

    #[test]
    fn ambiguous_when_runner_up_close() {
        // 0.92 vs 0.90 different ids, gap 0.02 < 0.05
        let scored = vec![make_scored(1, 0.92), make_scored(2, 0.90)];
        assert!(!is_unambiguous_winner(&scored, 0.05));
    }

    #[test]
    fn ambiguous_check_skips_same_id_higher_in_list() {
        // top is id=1 score 0.95; next entry is also id=1 (different lang
        // hit) with 0.85; further down id=2 with 0.93 — that's the runner-up
        // we care about. Gap = 0.02 → ambiguous.
        let scored = vec![
            make_scored(1, 0.95),
            make_scored(1, 0.85),
            make_scored(2, 0.93),
        ];
        assert!(!is_unambiguous_winner(&scored, 0.05));
    }

    #[test]
    fn unambiguous_for_empty_returns_false() {
        let scored: Vec<ScoredCandidate> = vec![];
        assert!(!is_unambiguous_winner(&scored, 0.05));
    }

    fn dummy_candidate(title: &str, original_title: Option<&str>, release_date: Option<&str>, popularity: f64) -> TmdbSearchResult {
        TmdbSearchResult {
            id: 1,
            title: title.to_string(),
            original_title: original_title.map(|t| t.to_string()),
            release_date: release_date.map(|d| d.to_string()),
            overview: None,
            poster_path: None,
            vote_average: None,
            vote_count: None,
            popularity: Some(popularity),
            genre_ids: None,
            original_language: None,
        }
    }

    #[test]
    fn exact_match_high_score() {
        let candidates = vec![dummy_candidate("Inception", Some("Inception"), Some("2010-07-16"), 80.0)];
        let scored = score_candidates("Inception", Some(2010), candidates);

        assert_eq!(scored.len(), 1);
        assert!(scored[0].score > 0.9, "score was {}", scored[0].score);
    }

    #[test]
    fn year_mismatch_lowers_score() {
        let correct = dummy_candidate("Inception", None, Some("2010-07-16"), 50.0);
        let mismatch = dummy_candidate("Inception", None, Some("2012-05-01"), 50.0);

        let scored = score_candidates("Inception", Some(2010), vec![correct, mismatch]);
        let correct_score = scored[0].score;
        let mismatch_score = scored[1].score;

        assert!(correct_score > mismatch_score, "expected correct year to score higher");
    }

    #[test]
    fn decide_auto_confirm() {
        let decision = decide_match(0.82, 0.8);
        assert_eq!(decision, MatchDecision::AutoConfirm);
    }

    #[test]
    fn decide_pending() {
        let decision = decide_match(0.6, 0.8);
        assert_eq!(decision, MatchDecision::Pending);
    }

    #[test]
    fn decide_failed() {
        let decision = decide_match(0.4, 0.8);
        assert_eq!(decision, MatchDecision::Failed);
    }

    #[test]
    fn decide_exactly_at_threshold_auto_confirms() {
        assert_eq!(decide_match(0.85, 0.85), MatchDecision::AutoConfirm);
    }

    #[test]
    fn decide_exactly_at_pending_floor_is_pending() {
        assert_eq!(decide_match(0.5, 0.85), MatchDecision::Pending);
    }

    #[test]
    fn decide_just_below_pending_floor_fails() {
        assert_eq!(decide_match(0.4999, 0.85), MatchDecision::Failed);
    }

    #[test]
    fn original_title_can_outscore_primary_title() {
        // parsed title in English; candidate primary is the localized (zh) title,
        // original_title is the English one — alt should drive the score.
        let candidate = dummy_candidate(
            "盗梦空间",
            Some("Inception"),
            Some("2010-07-16"),
            50.0,
        );
        let scored = score_candidates("Inception", Some(2010), vec![candidate]);
        assert!(
            scored[0].score > 0.8,
            "alt title match should still yield a strong score, got {}",
            scored[0].score
        );
    }

    #[test]
    fn off_by_one_year_yields_partial_credit() {
        // Property test (not a hardcoded value): off-by-one should sit between
        // exact-year and no-year-at-all, regardless of how the calibration is
        // tuned over time. Locking the magic number 0.75/0.85 here just rubber-
        // stamps whatever value was last picked; this asserts the actual
        // invariant we care about.
        let exact = dummy_candidate("Inception", None, Some("2010-07-16"), 0.0);
        let off1 = dummy_candidate("Inception", None, Some("2011-07-16"), 0.0);
        let no_year = dummy_candidate("Inception", None, None, 0.0);

        let s_exact = score_candidates("Inception", Some(2010), vec![exact])[0].score;
        let s_off1 = score_candidates("Inception", Some(2010), vec![off1])[0].score;
        let s_none = score_candidates("Inception", Some(2010), vec![no_year])[0].score;

        assert!(s_off1 < s_exact, "off-by-one ({}) should be less than exact ({})", s_off1, s_exact);
        assert!(s_off1 > s_none, "off-by-one ({}) should be more than no-year ({})", s_off1, s_none);
    }

    #[test]
    fn title_perfect_plus_off_by_one_auto_confirms_at_default_threshold() {
        // Behavior test for the festival/theatrical year-wobble fix.
        // Beau Travail premiered Venice 1999 / France 2000; same shape as Inu-oh,
        // 钢的琴, Through the Olive Trees. Without this, a perfectly-titled match
        // with a 1-year wobble would stick at "pending" forever — that's exactly
        // the 150-record bucket sitting at confidence ≈ 0.751 in production today.
        let c = dummy_candidate("Inception", None, Some("2011-07-16"), 0.0);
        let scored = score_candidates("Inception", Some(2010), vec![c]);
        assert_eq!(
            decide_match(scored[0].score, 0.85),
            MatchDecision::AutoConfirm,
            "title-perfect + off-by-one should auto-confirm at default threshold; got score {}",
            scored[0].score,
        );
    }

    #[test]
    fn missing_release_date_gives_no_year_score() {
        let c = dummy_candidate("Inception", None, None, 0.0);
        let scored = score_candidates("Inception", Some(2010), vec![c]);
        // No year-score, no popularity — only 0.6 title weight.
        assert!((scored[0].score - 0.6).abs() < 0.01, "got {}", scored[0].score);
    }

    #[test]
    fn empty_candidates_returns_empty() {
        let scored = score_candidates("anything", Some(2000), vec![]);
        assert!(scored.is_empty());
    }

    #[test]
    fn results_sorted_by_score_desc() {
        // Intentionally push the better match to index 1 so we can see sorting.
        let bad = dummy_candidate("Totally Unrelated", None, Some("1980-01-01"), 10.0);
        let good = dummy_candidate("Inception", None, Some("2010-07-16"), 90.0);
        let scored = score_candidates("Inception", Some(2010), vec![bad, good]);
        assert_eq!(scored[0].tmdb_result.title, "Inception");
        assert!(scored[0].score > scored[1].score);
    }

    #[test]
    fn popularity_capped_at_one_hundred() {
        // Two identical candidates differing only in popularity. The higher one
        // should score at most 0.1 more (popularity weight).
        let low = dummy_candidate("Test", None, Some("2000-01-01"), 50.0);
        let high = dummy_candidate("Test", None, Some("2000-01-01"), 5000.0);
        let scored = score_candidates("Test", Some(2000), vec![low, high]);
        // Both get title + year; only popularity differs.
        let diff = scored[0].score - scored[1].score;
        assert!(diff > 0.0 && diff <= 0.1 + 1e-9, "diff was {}", diff);
    }
}
