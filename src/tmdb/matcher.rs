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

/// Score TMDB search results against a parsed title and optional year.
///
/// Scoring breakdown:
/// - Title similarity (weight 0.6): best of `title` vs `original_title`
///   using normalized Levenshtein.
/// - Year match (weight 0.3): exact = 0.3, +/-1 = 0.15, else 0.0.
/// - Popularity boost (weight 0.1): (popularity / 100) capped at 1.0.
pub fn score_candidates(
    parsed_title: &str,
    parsed_year: Option<u16>,
    candidates: Vec<TmdbSearchResult>,
) -> Vec<ScoredCandidate> {
    let parsed_title_norm = parsed_title.to_lowercase();

    let mut scored: Vec<ScoredCandidate> = candidates
        .into_iter()
        .map(|candidate| {
            let title_similarity = {
                let primary = normalized_levenshtein(&parsed_title_norm, &candidate.title.to_lowercase());
                let alt = candidate
                    .original_title
                    .as_ref()
                    .map(|t| normalized_levenshtein(&parsed_title_norm, &t.to_lowercase()))
                    .unwrap_or(0.0);
                primary.max(alt) * 0.6
            };

            let year_score = parsed_year
                .and_then(|py| release_year(&candidate).map(|cy| (py, cy)))
                .map(|(py, cy)| {
                    if py == cy {
                        0.3
                    } else if (py as i32 - cy as i32).abs() == 1 {
                        0.15
                    } else {
                        0.0
                    }
                })
                .unwrap_or(0.0);

            let popularity_score = candidate
                .popularity
                .map(|p| (p / 100.0).min(1.0) * 0.1)
                .unwrap_or(0.0);

            let score = title_similarity + year_score + popularity_score;

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

#[cfg(test)]
mod tests {
    use super::*;

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
    fn off_by_one_year_partial_credit() {
        // Same title, only the year is off by one. Year-score is 0.15 (half of 0.3).
        let c = dummy_candidate("Inception", None, Some("2011-07-16"), 0.0);
        let scored = score_candidates("Inception", Some(2010), vec![c]);
        // 0.6 (title) + 0.15 (year +/-1) + 0.0 (no popularity) = 0.75
        assert!((scored[0].score - 0.75).abs() < 0.01, "got {}", scored[0].score);
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
