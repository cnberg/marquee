use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryIntent {
    #[serde(default)]
    pub constraints: Constraints,
    #[serde(default)]
    pub exclusions: Exclusions,
    #[serde(default)]
    pub preferences: Preferences,
    #[serde(default)]
    pub search_intents: Vec<String>,
    #[serde(default = "default_sort_rules")]
    pub sort_rules: Vec<SortRule>,
    #[serde(default = "default_query_type")]
    pub query_type: String,
    #[serde(default = "default_watched_policy")]
    pub watched_policy: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Constraints {
    #[serde(default)]
    pub year_range: YearRange,
    #[serde(default)]
    pub decades: Vec<i64>,
    #[serde(default)]
    pub languages: Vec<String>,
    #[serde(default)]
    pub genres: Vec<String>,
    #[serde(default)]
    pub countries: Vec<String>,
    #[serde(default)]
    pub directors: Vec<String>,
    #[serde(default)]
    pub cast: Vec<String>,
    #[serde(default)]
    pub keywords: Vec<String>,
    #[serde(default)]
    pub min_rating: Option<f64>,
    #[serde(default)]
    pub max_rating: Option<f64>,
    #[serde(default)]
    pub runtime_range: RuntimeRange,
    #[serde(default)]
    pub budget_tier: Option<String>,
    #[serde(default)]
    pub popularity_tier: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct YearRange {
    pub min: Option<i32>,
    pub max: Option<i32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RuntimeRange {
    pub min: Option<i32>,
    pub max: Option<i32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Exclusions {
    #[serde(default)]
    pub genres: Vec<String>,
    #[serde(default)]
    pub keywords: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Preferences {
    #[serde(default)]
    pub decades: Vec<i64>,
    #[serde(default)]
    pub genres: Vec<String>,
    #[serde(default)]
    pub countries: Vec<String>,
    #[serde(default)]
    pub languages: Vec<String>,
    #[serde(default)]
    pub directors: Vec<String>,
    #[serde(default)]
    pub keywords: Vec<String>,
    #[serde(default)]
    pub budget_tier: Option<String>,
    #[serde(default)]
    pub popularity_tier: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortRule {
    pub field: String,
    pub weight: f64,
    #[serde(default = "default_sort_order")]
    pub order: String,
}

fn default_sort_rules() -> Vec<SortRule> {
    vec![SortRule {
        field: "relevance".to_string(),
        weight: 1.0,
        order: default_sort_order(),
    }]
}

fn default_sort_order() -> String {
    "desc".to_string()
}

fn default_query_type() -> String {
    "semantic".to_string()
}

fn default_watched_policy() -> String {
    "neutral".to_string()
}

/// 校验和修正 LLM 返回的 QueryIntent。
/// 对不合法的值做截断/丢弃/降级，确保下游可安全使用。
/// available_genres: 库中实际存在的类型列表（中文），用于模糊匹配。
pub fn validate_intent(intent: &mut QueryIntent, original_query: &str, available_genres: &[String]) {
    let c = &mut intent.constraints;

    // year_range: min > max → 交换; 超出 1900-2030 → 截断
    if let (Some(min), Some(max)) = (c.year_range.min, c.year_range.max) {
        if min > max {
            c.year_range.min = Some(max);
            c.year_range.max = Some(min);
        }
    }
    if let Some(ref mut v) = c.year_range.min {
        *v = (*v).clamp(1900, 2030);
    }
    if let Some(ref mut v) = c.year_range.max {
        *v = (*v).clamp(1900, 2030);
    }

    // decades: 非 10 倍数 → 向下取整; 超范围 → 丢弃
    c.decades = c
        .decades
        .iter()
        .map(|d| (d / 10) * 10)
        .filter(|d| (1920..=2020).contains(d))
        .collect();

    // languages: 保留 2-3 字符的，粗略校验 ISO 639-1
    c.languages
        .retain(|l| l.len() >= 2 && l.len() <= 3 && l.chars().all(|c| c.is_ascii_lowercase()));

    // genres: 模糊匹配库中已有类型
    c.genres = c
        .genres
        .iter()
        .filter_map(|g| {
            if available_genres.contains(g) {
                return Some(g.clone());
            }
            available_genres
                .iter()
                .find(|ag| strsim::levenshtein(g, ag) <= 1)
                .cloned()
        })
        .collect();

    // countries: 保留 2 字符大写字母
    c.countries
        .retain(|co| co.len() == 2 && co.chars().all(|c| c.is_ascii_uppercase()));

    // min/max_rating: 截断到 0-10
    if let Some(ref mut v) = c.min_rating {
        *v = v.clamp(0.0, 10.0);
    }
    if let Some(ref mut v) = c.max_rating {
        *v = v.clamp(0.0, 10.0);
    }

    // runtime_range: 截断到 1-600
    if let Some(ref mut v) = c.runtime_range.min {
        *v = (*v).clamp(1, 600);
    }
    if let Some(ref mut v) = c.runtime_range.max {
        *v = (*v).clamp(1, 600);
    }

    // budget_tier: 必须是 low/medium/high
    if let Some(ref tier) = c.budget_tier {
        if !["low", "medium", "high"].contains(&tier.as_str()) {
            c.budget_tier = None;
        }
    }

    // popularity_tier: 必须是 niche/moderate/popular
    if let Some(ref tier) = c.popularity_tier {
        if !["niche", "moderate", "popular"].contains(&tier.as_str()) {
            c.popularity_tier = None;
        }
    }

    // watched_policy: 必须是 exclude/prefer/neutral
    if !["exclude", "prefer", "neutral"].contains(&intent.watched_policy.as_str()) {
        intent.watched_policy = "neutral".to_string();
    }

    // exclusions.genres: 同样做模糊匹配
    intent.exclusions.genres = intent
        .exclusions
        .genres
        .iter()
        .filter_map(|g| {
            if available_genres.contains(g) {
                return Some(g.clone());
            }
            available_genres
                .iter()
                .find(|ag| strsim::levenshtein(g, ag) <= 1)
                .cloned()
        })
        .collect();

    // preferences.decades: 同 constraints.decades 的校验逻辑
    intent.preferences.decades = intent
        .preferences
        .decades
        .iter()
        .map(|d| (d / 10) * 10)
        .filter(|d| (1920..=2020).contains(d))
        .collect();

    // preferences.genres: 模糊匹配
    intent.preferences.genres = intent
        .preferences
        .genres
        .iter()
        .filter_map(|g| {
            if available_genres.contains(g) {
                return Some(g.clone());
            }
            available_genres
                .iter()
                .find(|ag| strsim::levenshtein(g, ag) <= 1)
                .cloned()
        })
        .collect();

    // preferences.countries: 保留 2 字符大写字母
    intent
        .preferences
        .countries
        .retain(|co| co.len() == 2 && co.chars().all(|c| c.is_ascii_uppercase()));

    // preferences.languages: 保留 2-3 字符小写字母
    intent
        .preferences
        .languages
        .retain(|l| l.len() >= 2 && l.len() <= 3 && l.chars().all(|c| c.is_ascii_lowercase()));

    // preferences.budget_tier: 必须是 low/medium/high
    if let Some(ref tier) = intent.preferences.budget_tier {
        if !["low", "medium", "high"].contains(&tier.as_str()) {
            intent.preferences.budget_tier = None;
        }
    }

    // preferences.popularity_tier: 必须是 niche/moderate/popular
    if let Some(ref tier) = intent.preferences.popularity_tier {
        if !["niche", "moderate", "popular"].contains(&tier.as_str()) {
            intent.preferences.popularity_tier = None;
        }
    }

    // sort_rules: field 必须在枚举中，weight 归一化
    let valid_fields = ["relevance", "rating", "year", "popularity", "runtime"];
    intent.sort_rules.retain(|r| valid_fields.contains(&r.field.as_str()));
    if intent.sort_rules.is_empty() {
        intent.sort_rules = default_sort_rules();
    }
    // sort_rules.order: 必须是 asc/desc，否则默认 desc
    for rule in &mut intent.sort_rules {
        if !["asc", "desc"].contains(&rule.order.as_str()) {
            rule.order = "desc".to_string();
        }
    }
    let total_weight: f64 = intent.sort_rules.iter().map(|r| r.weight).sum();
    if total_weight > 0.0 && (total_weight - 1.0).abs() > 0.01 {
        for rule in &mut intent.sort_rules {
            rule.weight /= total_weight;
        }
    }

    // query_type: 必须在枚举中
    if !["keyword", "semantic", "mixed"].contains(&intent.query_type.as_str()) {
        intent.query_type = "semantic".to_string();
    }

    // search_intents: 为空 → 用原始 query
    if intent.search_intents.is_empty() {
        intent.search_intents = vec![original_query.to_string()];
    }

    // 所有 constraints 全空 → 强制 semantic
    let all_empty = c.year_range.min.is_none()
        && c.year_range.max.is_none()
        && c.decades.is_empty()
        && c.languages.is_empty()
        && c.genres.is_empty()
        && c.countries.is_empty()
        && c.directors.is_empty()
        && c.cast.is_empty()
        && c.keywords.is_empty()
        && c.min_rating.is_none()
        && c.max_rating.is_none()
        && c.runtime_range.min.is_none()
        && c.runtime_range.max.is_none()
        && c.budget_tier.is_none()
        && c.popularity_tier.is_none();
    if all_empty {
        intent.query_type = "semantic".to_string();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_intent() -> QueryIntent {
        // A non-empty intent so validate_intent does not force semantic fallback.
        QueryIntent {
            constraints: Constraints {
                year_range: YearRange {
                    min: Some(2000),
                    max: Some(2010),
                },
                ..Default::default()
            },
            exclusions: Exclusions::default(),
            preferences: Preferences::default(),
            search_intents: vec!["query".into()],
            sort_rules: default_sort_rules(),
            query_type: "keyword".into(),
            watched_policy: "neutral".into(),
        }
    }

    fn genres() -> Vec<String> {
        vec!["动作".into(), "喜剧".into(), "科幻".into()]
    }

    #[test]
    fn year_range_swap_when_inverted() {
        let mut i = base_intent();
        i.constraints.year_range = YearRange { min: Some(2020), max: Some(2000) };
        validate_intent(&mut i, "q", &genres());
        assert_eq!(i.constraints.year_range.min, Some(2000));
        assert_eq!(i.constraints.year_range.max, Some(2020));
    }

    #[test]
    fn year_range_clamped_to_window() {
        let mut i = base_intent();
        i.constraints.year_range = YearRange { min: Some(1800), max: Some(2999) };
        validate_intent(&mut i, "q", &genres());
        assert_eq!(i.constraints.year_range.min, Some(1900));
        assert_eq!(i.constraints.year_range.max, Some(2030));
    }

    #[test]
    fn decades_rounded_and_filtered() {
        let mut i = base_intent();
        i.constraints.decades = vec![1995, 2003, 1800, 2050];
        validate_intent(&mut i, "q", &genres());
        // 1995→1990 (kept), 2003→2000 (kept), 1800→1800 (dropped), 2050→2050 (dropped)
        assert_eq!(i.constraints.decades, vec![1990, 2000]);
    }

    #[test]
    fn languages_iso_639_1_filter() {
        let mut i = base_intent();
        i.constraints.languages = vec!["en".into(), "ZH".into(), "english".into(), "zh".into()];
        validate_intent(&mut i, "q", &genres());
        assert_eq!(i.constraints.languages, vec!["en", "zh"]);
    }

    #[test]
    fn genres_fuzzy_matched_to_available() {
        let mut i = base_intent();
        // "动作片" differs from "动作" by 1 char — should match.
        // "动作" exact match.
        // "悬疑" doesn't match any available → dropped.
        i.constraints.genres = vec!["动作".into(), "动作片".into(), "悬疑".into()];
        validate_intent(&mut i, "q", &genres());
        // Exact match kept as-is; fuzzy replaced with canonical "动作".
        assert_eq!(i.constraints.genres, vec!["动作", "动作"]);
    }

    #[test]
    fn countries_only_two_upper() {
        let mut i = base_intent();
        i.constraints.countries = vec!["US".into(), "usa".into(), "CN".into(), "china".into()];
        validate_intent(&mut i, "q", &genres());
        assert_eq!(i.constraints.countries, vec!["US", "CN"]);
    }

    #[test]
    fn rating_clamped_to_zero_ten() {
        let mut i = base_intent();
        i.constraints.min_rating = Some(-5.0);
        i.constraints.max_rating = Some(42.0);
        validate_intent(&mut i, "q", &genres());
        assert_eq!(i.constraints.min_rating, Some(0.0));
        assert_eq!(i.constraints.max_rating, Some(10.0));
    }

    #[test]
    fn invalid_tiers_dropped() {
        let mut i = base_intent();
        i.constraints.budget_tier = Some("mega".into());
        i.constraints.popularity_tier = Some("viral".into());
        validate_intent(&mut i, "q", &genres());
        assert!(i.constraints.budget_tier.is_none());
        assert!(i.constraints.popularity_tier.is_none());
    }

    #[test]
    fn valid_tiers_kept() {
        let mut i = base_intent();
        i.constraints.budget_tier = Some("medium".into());
        i.constraints.popularity_tier = Some("niche".into());
        validate_intent(&mut i, "q", &genres());
        assert_eq!(i.constraints.budget_tier.as_deref(), Some("medium"));
        assert_eq!(i.constraints.popularity_tier.as_deref(), Some("niche"));
    }

    #[test]
    fn invalid_watched_policy_falls_back_to_neutral() {
        let mut i = base_intent();
        i.watched_policy = "allergic".into();
        validate_intent(&mut i, "q", &genres());
        assert_eq!(i.watched_policy, "neutral");
    }

    #[test]
    fn sort_rules_weights_normalized_to_unity() {
        let mut i = base_intent();
        i.sort_rules = vec![
            SortRule { field: "relevance".into(), weight: 3.0, order: "desc".into() },
            SortRule { field: "rating".into(), weight: 1.0, order: "desc".into() },
        ];
        validate_intent(&mut i, "q", &genres());
        let total: f64 = i.sort_rules.iter().map(|r| r.weight).sum();
        assert!((total - 1.0).abs() < 0.01, "total weight {}", total);
    }

    #[test]
    fn invalid_sort_field_dropped_empty_rules_default() {
        let mut i = base_intent();
        i.sort_rules = vec![
            SortRule { field: "vibes".into(), weight: 1.0, order: "desc".into() },
        ];
        validate_intent(&mut i, "q", &genres());
        // All rules dropped → fallback to default_sort_rules().
        assert_eq!(i.sort_rules.len(), 1);
        assert_eq!(i.sort_rules[0].field, "relevance");
    }

    #[test]
    fn sort_rule_order_whitelisted() {
        let mut i = base_intent();
        i.sort_rules = vec![
            SortRule { field: "rating".into(), weight: 1.0, order: "random".into() },
        ];
        validate_intent(&mut i, "q", &genres());
        assert_eq!(i.sort_rules[0].order, "desc");
    }

    #[test]
    fn invalid_query_type_falls_back_to_semantic() {
        let mut i = base_intent();
        i.query_type = "telepathy".into();
        validate_intent(&mut i, "q", &genres());
        assert_eq!(i.query_type, "semantic");
    }

    #[test]
    fn empty_search_intents_fallback_to_original_query() {
        let mut i = base_intent();
        i.search_intents.clear();
        validate_intent(&mut i, "original prompt", &genres());
        assert_eq!(i.search_intents, vec!["original prompt".to_string()]);
    }

    #[test]
    fn all_empty_constraints_forces_semantic() {
        let mut i = base_intent();
        i.constraints = Constraints::default();
        i.query_type = "keyword".into();
        validate_intent(&mut i, "q", &genres());
        assert_eq!(i.query_type, "semantic");
    }

    #[test]
    fn exclusions_genres_fuzzy_matched() {
        let mut i = base_intent();
        i.exclusions.genres = vec!["动作片".into(), "nonsense".into()];
        validate_intent(&mut i, "q", &genres());
        assert_eq!(i.exclusions.genres, vec!["动作"]);
    }
}
