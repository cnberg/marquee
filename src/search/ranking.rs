use crate::db::UserMarkedMovie;
use crate::search::intent::{Constraints, Exclusions, Preferences, QueryIntent};
use std::collections::HashSet;

/// 一个候选电影，带有来源信息和分数
#[derive(Debug, Clone)]
pub struct RankedCandidate {
    pub movie_id: i64,
    pub tmdb_id: i64,
    pub title: String,
    pub year: Option<i64>,
    pub genres: Option<String>,
    pub director: Option<String>,
    pub language: Option<String>,
    pub country: Option<String>,
    pub overview: Option<String>,
    pub tmdb_rating: Option<f64>,
    pub runtime: Option<i64>,
    pub popularity: Option<f64>,
    pub budget: Option<i64>,
    pub keywords: Option<String>,
    pub cast: Option<String>,
    /// 来源：structured / semantic / both
    pub source: String,
    /// 是否属于用户本地片库
    pub in_library: bool,
    /// 语义相似度（0~1，越高越相似），仅语义召回有值
    pub semantic_score: f64,
}

/// 粗排：先约束过滤，再多维打分，取 top N。
pub fn coarse_rank(
    candidates: &mut Vec<RankedCandidate>,
    intent: &QueryIntent,
    top_n: usize,
    user_marks: &[UserMarkedMovie],
) {
    // Build user mark sets for collaborative recall
    let interested_ids: HashSet<i64> = user_marks
        .iter()
        .filter(|m| m.mark_type == "want" || m.mark_type == "favorite")
        .map(|m| m.movie_id)
        .collect();

    let watched_ids: HashSet<i64> = user_marks
        .iter()
        .filter(|m| m.mark_type == "watched")
        .map(|m| m.movie_id)
        .collect();

    // Step 1: 硬约束过滤。结构化召回 SQL 已经按 constraints 过滤过，但语义/协同
    // 召回的结果是绕过 constraints 进来的，必须在这里再过一遍，否则像
    // "周星驰的电影" 这种 cast 硬约束会被语义召回的"奇幻动画"冲垮。
    candidates.retain(|c| passes_constraints(c, &intent.constraints, &intent.exclusions));

    // Apply watched_policy
    match intent.watched_policy.as_str() {
        "exclude" => {
            candidates.retain(|c| !watched_ids.contains(&c.movie_id));
        }
        _ => {}
    }

    if candidates.is_empty() {
        return;
    }

    // Step 2: 多维打分
    // 收集 popularity 分布，用于三分位归一化
    let mut pop_values: Vec<f64> = candidates.iter().filter_map(|c| c.popularity).collect();
    pop_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let pop_p33 = percentile(&pop_values, 33.0);
    let pop_p66 = percentile(&pop_values, 66.0);

    for c in candidates.iter_mut() {
        let mut score = 0.0;
        for rule in &intent.sort_rules {
            let dim_score = match rule.field.as_str() {
                "relevance" => c.semantic_score,
                "rating" => c.tmdb_rating.unwrap_or(0.0) / 10.0,
                "year" => c
                    .year
                    .map(|y| (y as f64 - 1920.0) / 100.0)
                    .unwrap_or(0.0)
                    .clamp(0.0, 1.0),
                "popularity" => match c.popularity {
                    Some(p) if p >= pop_p66 => 1.0,
                    Some(p) if p >= pop_p33 => 0.5,
                    Some(_) => 0.0,
                    None => 0.0,
                },
                "runtime" => c
                    .runtime
                    .map(|r| (r as f64) / 300.0)
                    .unwrap_or(0.5)
                    .clamp(0.0, 1.0),
                _ => 0.0,
            };
            let adjusted = if rule.order == "asc" {
                1.0 - dim_score
            } else {
                dim_score
            };
            score += rule.weight * adjusted;
        }
        // Preference bonus: each matching preference adds a small bonus
        let pref_bonus = compute_preference_bonus(c, &intent.preferences);
        // User mark bonus: interested movies get +0.15, watched with "prefer" policy get +0.10
        let mark_bonus = if interested_ids.contains(&c.movie_id) {
            0.15
        } else if intent.watched_policy == "prefer" && watched_ids.contains(&c.movie_id) {
            0.10
        } else {
            0.0
        };

        score += pref_bonus + mark_bonus;
        c.semantic_score = score; // 复用 semantic_score 字段存储最终分数
    }

    // Sort by score descending
    candidates.sort_by(|a, b| {
        b.semantic_score
            .partial_cmp(&a.semantic_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    candidates.truncate(top_n);
}

fn passes_constraints(
    c: &RankedCandidate,
    constraints: &Constraints,
    exclusions: &Exclusions,
) -> bool {
    // Year range
    if let Some(min) = constraints.year_range.min {
        if let Some(year) = c.year {
            if year < min as i64 {
                return false;
            }
        }
    }
    if let Some(max) = constraints.year_range.max {
        if let Some(year) = c.year {
            if year > max as i64 {
                return false;
            }
        }
    }

    // Decades: (year/10)*10 must be in the requested set
    if !constraints.decades.is_empty() {
        match c.year {
            Some(year) => {
                let decade = (year / 10) * 10;
                if !constraints.decades.contains(&decade) {
                    return false;
                }
            }
            None => return false,
        }
    }

    // Languages
    if !constraints.languages.is_empty() {
        match c.language {
            Some(ref lang) => {
                if !constraints.languages.iter().any(|l| l == lang) {
                    return false;
                }
            }
            None => return false,
        }
    }

    // Countries
    if !constraints.countries.is_empty() {
        match c.country {
            Some(ref country) => {
                if !constraints.countries.iter().any(|cc| cc == country) {
                    return false;
                }
            }
            None => return false,
        }
    }

    // Genres: at least one of the candidate's genres must match (mirrors structured_recall's
    // EXISTS json_each WHERE value IN (...) semantics)
    if !constraints.genres.is_empty() {
        if !json_array_intersects(c.genres.as_deref(), &constraints.genres) {
            return false;
        }
    }

    // Directors: exact match against c.director (matches structured_recall's IN clause)
    if !constraints.directors.is_empty() {
        match c.director {
            Some(ref dir) => {
                if !constraints.directors.iter().any(|d| d == dir) {
                    return false;
                }
            }
            None => return false,
        }
    }

    // Cast: at least one of the requested cast members must appear in the candidate's cast.
    // This is the case that made "周星驰的电影" leak unrelated semantic neighbours
    // before the fix.
    if !constraints.cast.is_empty() {
        if !json_array_intersects(c.cast.as_deref(), &constraints.cast) {
            return false;
        }
    }

    // Keywords (constraints, not exclusions)
    if !constraints.keywords.is_empty() {
        if !json_array_intersects(c.keywords.as_deref(), &constraints.keywords) {
            return false;
        }
    }

    // Min rating
    if let Some(min_r) = constraints.min_rating {
        if c.tmdb_rating.unwrap_or(0.0) < min_r {
            return false;
        }
    }
    if let Some(max_r) = constraints.max_rating {
        if c.tmdb_rating.unwrap_or(10.0) > max_r {
            return false;
        }
    }

    // Runtime range
    if let Some(min_rt) = constraints.runtime_range.min {
        if let Some(rt) = c.runtime {
            if rt < min_rt as i64 {
                return false;
            }
        }
    }
    if let Some(max_rt) = constraints.runtime_range.max {
        if let Some(rt) = c.runtime {
            if rt > max_rt as i64 {
                return false;
            }
        }
    }

    // Budget tier (same thresholds as structured_recall)
    if let Some(ref tier) = constraints.budget_tier {
        match c.budget {
            Some(b) => {
                let actual = if b > 50_000_000 {
                    "high"
                } else if b >= 5_000_000 {
                    "medium"
                } else if b > 0 {
                    "low"
                } else {
                    return false; // unknown budget can't satisfy a tier requirement
                };
                if actual != tier {
                    return false;
                }
            }
            None => return false,
        }
    }

    // popularity_tier intentionally not enforced here: structured_recall uses a
    // global percentile (top/bottom third) that we cannot recompute per-candidate
    // without the full distribution. The LLM almost always puts popularity_tier
    // into preferences (soft) anyway, so this is a deliberate gap, not an oversight.

    // Exclusions: genres
    if !exclusions.genres.is_empty() {
        if let Some(ref genres_json) = c.genres {
            if let Ok(genres) = serde_json::from_str::<Vec<String>>(genres_json) {
                if genres.iter().any(|g| exclusions.genres.contains(g)) {
                    return false;
                }
            }
        }
    }

    // Exclusions: keywords
    if !exclusions.keywords.is_empty() {
        if let Some(ref kw_json) = c.keywords {
            if let Ok(kws) = serde_json::from_str::<Vec<String>>(kw_json) {
                if kws.iter().any(|k| exclusions.keywords.contains(k)) {
                    return false;
                }
            }
        }
    }

    true
}

/// JSON-array column ∩ wanted: true iff at least one element of `wanted` appears
/// in the JSON array. Returns false when the column is NULL or unparseable, so
/// candidates without the data cannot satisfy a hard requirement.
fn json_array_intersects(json_col: Option<&str>, wanted: &[String]) -> bool {
    let Some(s) = json_col else { return false };
    let Ok(values) = serde_json::from_str::<Vec<String>>(s) else { return false };
    values.iter().any(|v| wanted.iter().any(|w| w == v))
}

/// 计算候选与 preferences 的匹配度加分（0.0~0.15）。
/// 每命中一个维度加 0.03，最高 0.15（5 个维度封顶）。
fn compute_preference_bonus(c: &RankedCandidate, prefs: &Preferences) -> f64 {
    let mut hits = 0u32;

    // decades
    if !prefs.decades.is_empty() {
        if let Some(year) = c.year {
            let decade = (year / 10) * 10;
            if prefs.decades.contains(&decade) {
                hits += 1;
            }
        }
    }

    // genres
    if !prefs.genres.is_empty() {
        if let Some(ref genres_json) = c.genres {
            if let Ok(genres) = serde_json::from_str::<Vec<String>>(genres_json) {
                if genres.iter().any(|g| prefs.genres.contains(g)) {
                    hits += 1;
                }
            }
        }
    }

    // countries
    if !prefs.countries.is_empty() {
        if let Some(ref country) = c.country {
            if prefs.countries.iter().any(|pc| pc == country) {
                hits += 1;
            }
        }
    }

    // languages
    if !prefs.languages.is_empty() {
        if let Some(ref lang) = c.language {
            if prefs.languages.iter().any(|pl| pl == lang) {
                hits += 1;
            }
        }
    }

    // keywords
    if !prefs.keywords.is_empty() {
        if let Some(ref kw_json) = c.keywords {
            if let Ok(kws) = serde_json::from_str::<Vec<String>>(kw_json) {
                if kws.iter().any(|k| prefs.keywords.contains(k)) {
                    hits += 1;
                }
            }
        }
    }

    // directors
    if !prefs.directors.is_empty() {
        if let Some(ref dir) = c.director {
            if prefs.directors.iter().any(|pd| pd == dir) {
                hits += 1;
            }
        }
    }

    // budget_tier
    if let Some(ref pref_tier) = prefs.budget_tier {
        if let Some(budget) = c.budget {
            let actual_tier = if budget < 5_000_000 {
                "low"
            } else if budget < 50_000_000 {
                "medium"
            } else {
                "high"
            };
            if pref_tier == actual_tier {
                hits += 1;
            }
        }
    }

    // popularity_tier — 使用粗略阈值
    if let Some(ref pref_tier) = prefs.popularity_tier {
        if let Some(pop) = c.popularity {
            let actual_tier = if pop < 10.0 {
                "niche"
            } else if pop < 50.0 {
                "moderate"
            } else {
                "popular"
            };
            if pref_tier == actual_tier {
                hits += 1;
            }
        }
    }

    (hits.min(5) as f64) * 0.03
}

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((pct / 100.0) * sorted.len() as f64) as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::intent::{
        Constraints, Exclusions, Preferences, QueryIntent, SortRule, YearRange,
    };

    fn candidate(id: i64, title: &str, year: Option<i64>) -> RankedCandidate {
        RankedCandidate {
            movie_id: id,
            tmdb_id: id,
            title: title.to_string(),
            year,
            genres: None,
            director: None,
            language: None,
            country: None,
            overview: None,
            tmdb_rating: None,
            runtime: None,
            popularity: None,
            budget: None,
            keywords: None,
            cast: None,
            source: "semantic".into(),
            in_library: true,
            semantic_score: 0.5,
        }
    }

    fn basic_intent() -> QueryIntent {
        QueryIntent {
            constraints: Constraints::default(),
            exclusions: Exclusions::default(),
            preferences: Preferences::default(),
            search_intents: vec![],
            sort_rules: vec![SortRule {
                field: "relevance".into(),
                weight: 1.0,
                order: "desc".into(),
            }],
            query_type: "semantic".into(),
            watched_policy: "neutral".into(),
        }
    }

    // --- passes_constraints ---

    #[test]
    fn year_range_filters_out_old_movies() {
        let mut cands = vec![
            candidate(1, "Old", Some(1950)),
            candidate(2, "Modern", Some(2010)),
        ];
        let mut intent = basic_intent();
        intent.constraints.year_range = YearRange { min: Some(2000), max: None };
        coarse_rank(&mut cands, &intent, 10, &[]);
        assert_eq!(cands.len(), 1);
        assert_eq!(cands[0].movie_id, 2);
    }

    #[test]
    fn rating_min_filter() {
        let mut a = candidate(1, "A", Some(2010));
        a.tmdb_rating = Some(6.0);
        let mut b = candidate(2, "B", Some(2010));
        b.tmdb_rating = Some(8.5);
        let mut cands = vec![a, b];
        let mut intent = basic_intent();
        intent.constraints.min_rating = Some(7.0);
        coarse_rank(&mut cands, &intent, 10, &[]);
        assert_eq!(cands.len(), 1);
        assert_eq!(cands[0].movie_id, 2);
    }

    // Regression: "周星驰的经典电影，一定要周星驰的" previously leaked unrelated
    // semantic neighbours (e.g. 心灵奇旅) because passes_constraints ignored
    // constraints.cast entirely. Only candidates whose cast includes one of the
    // requested names may survive.
    #[test]
    fn cast_constraint_drops_candidates_without_match() {
        let mut stephen = candidate(1, "大话西游", Some(1995));
        stephen.cast = Some("[\"周星驰\",\"吴孟达\"]".into());
        let mut pixar = candidate(2, "心灵奇旅", Some(2020));
        pixar.cast = Some("[\"Jamie Foxx\",\"Tina Fey\"]".into());
        let mut no_cast_data = candidate(3, "Unknown", Some(2010));
        no_cast_data.cast = None;
        let mut cands = vec![stephen, pixar, no_cast_data];
        let mut intent = basic_intent();
        intent.constraints.cast = vec!["周星驰".into()];
        coarse_rank(&mut cands, &intent, 10, &[]);
        assert_eq!(cands.len(), 1);
        assert_eq!(cands[0].movie_id, 1);
    }

    #[test]
    fn director_constraint_requires_exact_match() {
        let mut a = candidate(1, "A", Some(2010));
        a.director = Some("王家卫".into());
        let mut b = candidate(2, "B", Some(2010));
        b.director = Some("张艺谋".into());
        let mut cands = vec![a, b];
        let mut intent = basic_intent();
        intent.constraints.directors = vec!["王家卫".into()];
        coarse_rank(&mut cands, &intent, 10, &[]);
        assert_eq!(cands.len(), 1);
        assert_eq!(cands[0].movie_id, 1);
    }

    #[test]
    fn genre_constraint_requires_intersection() {
        let mut sci_fi = candidate(1, "SciFi", Some(2010));
        sci_fi.genres = Some("[\"科幻\",\"动作\"]".into());
        let mut romance = candidate(2, "Romance", Some(2010));
        romance.genres = Some("[\"爱情\"]".into());
        let mut cands = vec![sci_fi, romance];
        let mut intent = basic_intent();
        intent.constraints.genres = vec!["科幻".into()];
        coarse_rank(&mut cands, &intent, 10, &[]);
        assert_eq!(cands.len(), 1);
        assert_eq!(cands[0].movie_id, 1);
    }

    #[test]
    fn decade_constraint_filters_wrong_era() {
        let nineties = candidate(1, "90s", Some(1995));
        let twenties = candidate(2, "20s", Some(2022));
        let mut cands = vec![nineties, twenties];
        let mut intent = basic_intent();
        intent.constraints.decades = vec![1990];
        coarse_rank(&mut cands, &intent, 10, &[]);
        assert_eq!(cands.len(), 1);
        assert_eq!(cands[0].movie_id, 1);
    }

    #[test]
    fn language_constraint_filters() {
        let mut zh = candidate(1, "Z", Some(2010));
        zh.language = Some("zh".into());
        let mut en = candidate(2, "E", Some(2010));
        en.language = Some("en".into());
        let mut cands = vec![zh, en];
        let mut intent = basic_intent();
        intent.constraints.languages = vec!["zh".into()];
        coarse_rank(&mut cands, &intent, 10, &[]);
        assert_eq!(cands.len(), 1);
        assert_eq!(cands[0].movie_id, 1);
    }

    #[test]
    fn country_constraint_filters() {
        let mut cn = candidate(1, "C", Some(2010));
        cn.country = Some("CN".into());
        let mut us = candidate(2, "U", Some(2010));
        us.country = Some("US".into());
        let mut cands = vec![cn, us];
        let mut intent = basic_intent();
        intent.constraints.countries = vec!["CN".into()];
        coarse_rank(&mut cands, &intent, 10, &[]);
        assert_eq!(cands.len(), 1);
        assert_eq!(cands[0].movie_id, 1);
    }

    #[test]
    fn budget_tier_constraint_filters() {
        let mut low = candidate(1, "L", Some(2010));
        low.budget = Some(1_000_000);
        let mut high = candidate(2, "H", Some(2010));
        high.budget = Some(200_000_000);
        let mut cands = vec![low, high];
        let mut intent = basic_intent();
        intent.constraints.budget_tier = Some("high".into());
        coarse_rank(&mut cands, &intent, 10, &[]);
        assert_eq!(cands.len(), 1);
        assert_eq!(cands[0].movie_id, 2);
    }

    #[test]
    fn keyword_constraint_requires_intersection() {
        let mut a = candidate(1, "A", Some(2010));
        a.keywords = Some("[\"kung fu\",\"comedy\"]".into());
        let mut b = candidate(2, "B", Some(2010));
        b.keywords = Some("[\"noir\"]".into());
        let mut cands = vec![a, b];
        let mut intent = basic_intent();
        intent.constraints.keywords = vec!["kung fu".into()];
        coarse_rank(&mut cands, &intent, 10, &[]);
        assert_eq!(cands.len(), 1);
        assert_eq!(cands[0].movie_id, 1);
    }

    #[test]
    fn candidate_missing_data_cannot_satisfy_hard_constraint() {
        // cast column is NULL → can't prove it includes 周星驰 → drop.
        let mut no_cast = candidate(1, "Unknown", Some(2010));
        no_cast.cast = None;
        let mut cands = vec![no_cast];
        let mut intent = basic_intent();
        intent.constraints.cast = vec!["周星驰".into()];
        coarse_rank(&mut cands, &intent, 10, &[]);
        assert!(cands.is_empty());
    }

    #[test]
    fn excluded_genre_drops_candidate() {
        let mut a = candidate(1, "Horror", Some(2010));
        a.genres = Some("[\"恐怖\",\"惊悚\"]".into());
        let mut b = candidate(2, "Drama", Some(2010));
        b.genres = Some("[\"剧情\"]".into());
        let mut cands = vec![a, b];
        let mut intent = basic_intent();
        intent.exclusions.genres = vec!["恐怖".into()];
        coarse_rank(&mut cands, &intent, 10, &[]);
        assert_eq!(cands.len(), 1);
        assert_eq!(cands[0].movie_id, 2);
    }

    // --- watched_policy ---

    #[test]
    fn watched_policy_exclude_drops_watched() {
        let mut cands = vec![
            candidate(1, "Seen", Some(2010)),
            candidate(2, "Fresh", Some(2010)),
        ];
        let mut intent = basic_intent();
        intent.watched_policy = "exclude".into();
        let marks = vec![UserMarkedMovie {
            movie_id: 1,
            mark_type: "watched".into(),
            title: "Seen".into(),
            year: None,
            genres: None,
            director: None,
            country: None,
            language: None,
        }];
        coarse_rank(&mut cands, &intent, 10, &marks);
        assert_eq!(cands.len(), 1);
        assert_eq!(cands[0].movie_id, 2);
    }

    #[test]
    fn interested_movie_gets_mark_bonus() {
        // Two candidates with the same relevance. The "want"-marked one should
        // sort first because of the +0.15 mark bonus.
        let mut a = candidate(1, "Normal", Some(2010));
        a.semantic_score = 0.5;
        let mut b = candidate(2, "WantedOne", Some(2010));
        b.semantic_score = 0.5;
        let mut cands = vec![a, b];
        let intent = basic_intent();
        let marks = vec![UserMarkedMovie {
            movie_id: 2,
            mark_type: "want".into(),
            title: "WantedOne".into(),
            year: None,
            genres: None,
            director: None,
            country: None,
            language: None,
        }];
        coarse_rank(&mut cands, &intent, 10, &marks);
        assert_eq!(cands[0].movie_id, 2);
        // Final score should include the 0.15 mark bonus on top of relevance.
        assert!((cands[0].semantic_score - 0.65).abs() < 1e-6);
    }

    // --- sort_rules ---

    #[test]
    fn top_n_truncates_results() {
        let mut cands: Vec<_> = (0..5)
            .map(|i| {
                let mut c = candidate(i, "M", Some(2010));
                c.semantic_score = i as f64 * 0.1;
                c
            })
            .collect();
        let intent = basic_intent();
        coarse_rank(&mut cands, &intent, 3, &[]);
        assert_eq!(cands.len(), 3);
        // Highest scores first.
        assert_eq!(cands[0].movie_id, 4);
        assert_eq!(cands[1].movie_id, 3);
        assert_eq!(cands[2].movie_id, 2);
    }

    #[test]
    fn rating_sort_rule_ranks_by_tmdb_rating() {
        let mut low = candidate(1, "Low", Some(2010));
        low.tmdb_rating = Some(5.0);
        let mut high = candidate(2, "High", Some(2010));
        high.tmdb_rating = Some(9.0);
        let mut cands = vec![low, high];
        let mut intent = basic_intent();
        intent.sort_rules = vec![SortRule {
            field: "rating".into(),
            weight: 1.0,
            order: "desc".into(),
        }];
        coarse_rank(&mut cands, &intent, 10, &[]);
        assert_eq!(cands[0].movie_id, 2);
    }

    #[test]
    fn asc_order_inverts_ranking() {
        let mut old = candidate(1, "Old", Some(1930));
        let mut new = candidate(2, "New", Some(2020));
        old.tmdb_rating = Some(7.0);
        new.tmdb_rating = Some(7.0);
        let mut cands = vec![old, new];
        let mut intent = basic_intent();
        intent.sort_rules = vec![SortRule {
            field: "year".into(),
            weight: 1.0,
            order: "asc".into(),
        }];
        coarse_rank(&mut cands, &intent, 10, &[]);
        // asc: older year gets higher adjusted score.
        assert_eq!(cands[0].movie_id, 1);
    }

    // --- preferences bonus ---

    #[test]
    fn preference_genre_hit_adds_small_bonus() {
        let mut hit = candidate(1, "Match", Some(2010));
        hit.genres = Some("[\"科幻\"]".into());
        hit.semantic_score = 0.5;
        let mut miss = candidate(2, "NoMatch", Some(2010));
        miss.genres = Some("[\"爱情\"]".into());
        miss.semantic_score = 0.5;
        let mut cands = vec![hit, miss];
        let mut intent = basic_intent();
        intent.preferences.genres = vec!["科幻".into()];
        coarse_rank(&mut cands, &intent, 10, &[]);
        assert_eq!(cands[0].movie_id, 1);
        // One preference hit = +0.03 bonus.
        assert!((cands[0].semantic_score - 0.53).abs() < 1e-6);
        assert!((cands[1].semantic_score - 0.5).abs() < 1e-6);
    }

    // --- edge cases ---

    #[test]
    fn empty_candidates_is_noop() {
        let mut cands: Vec<RankedCandidate> = vec![];
        let intent = basic_intent();
        coarse_rank(&mut cands, &intent, 10, &[]);
        assert!(cands.is_empty());
    }

    #[test]
    fn all_candidates_filtered_leaves_empty() {
        let mut cands = vec![candidate(1, "Old", Some(1950))];
        let mut intent = basic_intent();
        intent.constraints.year_range = YearRange { min: Some(2020), max: None };
        coarse_rank(&mut cands, &intent, 10, &[]);
        assert!(cands.is_empty());
    }
}
