use crate::douban::parser::DoubanRecord;
use crate::tmdb::client::{TmdbClient, TmdbSearchResult};
use crate::tmdb::matcher::{decide_match, score_candidates, MatchDecision};
use std::collections::HashMap;

/// Outcome of attempting to bind a single Douban record to a TMDB movie.
/// `Found` is what the importer wants — it's now this caller's job to look the
/// movie up by tmdb_id, optionally create a row with `source='related'`, enqueue
/// a `tmdb_fetch` task and write the user's mark.
pub enum DoubanMatch {
    Found {
        tmdb_id: i64,
        tmdb_title: String,
        tmdb_original_title: Option<String>,
        tmdb_year: Option<i64>,
    },
    Pending,
    Failed,
}

/// Search TMDB with whichever of (zh, en) titles are present, then pick the
/// best candidate using the existing TMDB scorer. Threshold semantics mirror
/// the scanner pipeline: `>= auto_confirm_threshold` → `Found`, otherwise →
/// `Pending`. We never return `Failed` from here unless the TMDB call itself
/// errored badly enough that the candidate set is empty AND scoring couldn't
/// run — those go into the importer's pending list for the user to deal with.
pub async fn match_douban_record(
    tmdb: &TmdbClient,
    record: &DoubanRecord,
    auto_confirm_threshold: f64,
) -> DoubanMatch {
    let year_u32 = record.year.and_then(|y| u32::try_from(y).ok());

    let mut all_results: Vec<TmdbSearchResult> = Vec::new();

    // Pass 1 — Chinese title against zh-CN search
    if let Some(zh) = record.parsed_title_zh.as_deref() {
        let r = tmdb.search_movie_with_lang(zh, year_u32, "zh-CN").await.unwrap_or_default();
        all_results.extend(r);
    }

    // Pass 2 — English title against en-US search
    if let Some(en) = record.parsed_title_en.as_deref() {
        let r = tmdb.search_movie_with_lang(en, year_u32, "en-US").await.unwrap_or_default();
        all_results.extend(r);
    }

    // Fallback: if neither parsed title produced anything (or both were absent),
    // try the raw title field. Some Douban rows have only a single segment that
    // didn't classify cleanly as zh or en (e.g. all-Latin franchise names with
    // unusual punctuation).
    if all_results.is_empty() {
        let r = tmdb.search_movie_with_lang(&record.raw_title, year_u32, "zh-CN").await.unwrap_or_default();
        all_results.extend(r);
    }

    if all_results.is_empty() {
        return DoubanMatch::Failed;
    }

    // Score against every parsed title we've got, take best per tmdb_id.
    let mut query_titles: Vec<String> = Vec::new();
    if let Some(zh) = record.parsed_title_zh.as_deref() {
        query_titles.push(zh.to_string());
    }
    if let Some(en) = record.parsed_title_en.as_deref() {
        query_titles.push(en.to_string());
    }
    if query_titles.is_empty() {
        query_titles.push(record.raw_title.clone());
    }

    let mut best: HashMap<i64, (f64, TmdbSearchResult)> = HashMap::new();
    for qt in &query_titles {
        let scored = score_candidates(qt, year_u32.map(|y| y as u16), all_results.clone());
        for sc in scored {
            let entry = best.entry(sc.tmdb_result.id).or_insert((0.0, sc.tmdb_result.clone()));
            if sc.score > entry.0 {
                entry.0 = sc.score;
                entry.1 = sc.tmdb_result;
            }
        }
    }

    let Some((_, (top_score, top))) = best
        .iter()
        .max_by(|a, b| a.1 .0.partial_cmp(&b.1 .0).unwrap_or(std::cmp::Ordering::Equal))
    else {
        return DoubanMatch::Failed;
    };

    let top_score = *top_score;
    let top = top.clone();

    let tmdb_year = top
        .release_date
        .as_ref()
        .and_then(|d| d.get(0..4))
        .and_then(|y| y.parse::<i64>().ok());

    match decide_match(top_score, auto_confirm_threshold) {
        MatchDecision::AutoConfirm => DoubanMatch::Found {
            tmdb_id: top.id,
            tmdb_title: top.title.clone(),
            tmdb_original_title: top.original_title.clone(),
            tmdb_year,
        },
        MatchDecision::Pending | MatchDecision::Failed => DoubanMatch::Pending,
    }
}
