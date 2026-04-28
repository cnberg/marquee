
use std::path::Path;
use std::sync::Arc;
use sqlx::SqlitePool;
use tokio::time::{interval, Duration};

use crate::config::Config;
use crate::db::queries;
use crate::embedding::{EmbeddingModel, EmbeddingStore, KeywordDict};
use crate::llm::LlmClient;
use crate::qbittorrent::QbtClient;
use crate::scanner::{parser, sidecar, walker};
use crate::tmdb::client::{TmdbClient, TMDB_IMG_BASE};
use crate::tmdb::matcher::{decide_match, is_unambiguous_winner, score_candidates, MatchDecision};

/// 当前 tmdb_fetch pipeline 写入的数据版本。每次 `process_tmdb_fetch_tasks`
/// 的**输出范围**发生实质变化（新增字段、新增关联表、字段语义调整）时 +1。
/// 变更后 `refresh_stale_movies_worker` 会按 `refresh_interval_hours` 的节奏
/// 把所有 `tmdb_fetch_version < CURRENT_TMDB_FETCH_VERSION` 的电影重抓一遍。
///
/// 版本历史：
/// - v1：首个受版本管理的 pipeline。包含 similar/recommendations 写入
///   `related_movies` 表、`cast` 对象化（`{name, tmdb_person_id, ...}`）、
///   `update_movie_record` 完整的 TMDB full 字段集合。
pub const CURRENT_TMDB_FETCH_VERSION: i32 = 1;

pub async fn start_workers(pool: SqlitePool, config: Config) {
    let tmdb_client = TmdbClient::new(&config.tmdb.api_key, &config.tmdb.language, config.tmdb.proxy.as_deref());

    // Recover from previous process exit: any task left in `running` state is
    // an orphan from a crashed/restarted worker. `claim_next_task` only picks
    // `pending` tasks, so without this reset orphans stay stuck forever and
    // their dirs never get a final mapping.
    match queries::requeue_stale_running_tasks(&pool).await {
        Ok(0) => {}
        Ok(n) => tracing::warn!(requeued = n, "requeued stale running tasks from previous run"),
        Err(err) => tracing::error!("failed to requeue stale running tasks: {}", err),
    }

    // Scan worker
    let scan_pool = pool.clone();
    let scan_config = config.clone();
    tokio::spawn(async move {
        let mut timer = interval(Duration::from_secs(
            scan_config.scan.interval_hours as u64 * 3600,
        ));

        loop {
            timer.tick().await;
            tracing::info!("starting scheduled scan");

            if let Err(err) = run_scan_cycle(&scan_pool, &scan_config).await {
                tracing::error!("scan cycle failed: {}", err);
            }
        }
    });

    // TMDB search worker
    let poll_secs = config.scan.worker_poll_secs;
    let search_pool = pool.clone();
    let search_tmdb = tmdb_client.clone();
    let search_config = config.clone();
    tokio::spawn(async move {
        let mut timer = interval(Duration::from_secs(poll_secs));

        loop {
            timer.tick().await;
            process_tmdb_search_tasks(&search_pool, &search_tmdb, &search_config).await;
        }
    });

    // TMDB fetch worker
    let fetch_pool = pool.clone();
    let fetch_tmdb = tmdb_client;
    tokio::spawn(async move {
        let mut timer = interval(Duration::from_secs(poll_secs));

        loop {
            timer.tick().await;
            process_tmdb_fetch_tasks(&fetch_pool, &fetch_tmdb).await;
        }
    });

    // Stale-movie refresh worker: 每隔 refresh_interval_hours 选一批版本落后的
    // 电影入 tmdb_fetch 队列，让 pipeline 升级后的能力（新字段、related_movies
    // 关联等）自动铺到已经入库的老电影上。真正的 TMDB 调用仍由上面的 fetch worker
    // 消费，这里只管"选人 + 入队"。
    let refresh_pool = pool.clone();
    let refresh_config = config.clone();
    tokio::spawn(async move {
        let period = refresh_config
            .scan
            .refresh_interval_hours
            .max(1) as u64
            * 3600;
        let batch = refresh_config.scan.refresh_batch_size;
        let mut timer = interval(Duration::from_secs(period));

        loop {
            timer.tick().await;
            refresh_stale_movies(&refresh_pool, batch).await;
        }
    });

    // qBittorrent poll worker (only if enabled)
    if config.qbittorrent.enabled {
        let qbt_pool = pool.clone();
        let qbt_config = config.qbittorrent.clone();
        tokio::spawn(async move {
            // Wait for startup (migrations, embedding backfill) to finish before first poll
            tokio::time::sleep(Duration::from_secs(30)).await;
            let period = qbt_config.poll_interval_hours.max(1) as u64 * 3600;

            loop {
                tracing::info!("starting qBittorrent poll");
                if let Err(e) = poll_qbittorrent(&qbt_pool, &qbt_config).await {
                    tracing::error!("qBittorrent poll failed: {}", e);
                }
                tokio::time::sleep(Duration::from_secs(period)).await;
            }
        });
    }
}

/// Spawn translation + embedding-rebuild workers. Called separately from
/// [`start_workers`] because these depend on the LLM client, embedding model,
/// and `KeywordDict` cache, which are initialised after the basic
/// scan/TMDB-fetch workers boot.
pub fn start_translation_workers(
    pool: SqlitePool,
    llm: LlmClient,
    model: Arc<EmbeddingModel>,
    store: Arc<EmbeddingStore>,
    dict: KeywordDict,
) {
    // Keyword dictionary translation: 50 entries per batch every 60s.
    {
        let pool = pool.clone();
        let llm = llm.clone();
        let dict = dict.clone();
        tokio::spawn(async move {
            let mut timer = interval(Duration::from_secs(60));
            loop {
                timer.tick().await;
                crate::worker::translation::process_translation_batch(&pool, &llm, &dict).await;
            }
        });
    }

    // Overview translation: 10 movies per batch every 120s (richer payload
    // per row → smaller batch keeps JSON output reliable).
    {
        let pool = pool.clone();
        let llm = llm.clone();
        tokio::spawn(async move {
            let mut timer = interval(Duration::from_secs(120));
            loop {
                timer.tick().await;
                crate::worker::overview_translation::process_overview_batch(&pool, &llm).await;
            }
        });
    }

    // Embedding rebuild: 64 movies sampled per 30s. Drift detection is cheap
    // when there's nothing to do (one HashMap lookup chain + one lancedb
    // get-by-id). When translations land, this picks up the changed text and
    // re-embeds within ~30s.
    {
        let pool = pool.clone();
        let model = model.clone();
        let store = store.clone();
        let dict = dict.clone();
        tokio::spawn(async move {
            let mut timer = interval(Duration::from_secs(30));
            loop {
                timer.tick().await;
                crate::worker::embedding_rebuild::process_rebuild_batch(
                    &pool, &model, &store, &dict,
                )
                .await;
            }
        });
    }
}

/// 一轮"版本过期重抓"：查出最多 `batch_size` 部 `tmdb_fetch_version` 落后于
/// `CURRENT_TMDB_FETCH_VERSION` 的电影，逐条派发一个 `tmdb_fetch` 任务。
/// library 行带 `fetch_related:true`（需要补 related 关联），related 行带
/// `fetch_related:false`（和首次 seed 时一致，避免 related-of-related 滚雪球）。
pub async fn refresh_stale_movies(pool: &SqlitePool, batch_size: u32) {
    if batch_size == 0 {
        return;
    }

    let rows = match queries::claim_stale_movies(pool, CURRENT_TMDB_FETCH_VERSION, batch_size).await
    {
        Ok(rows) => rows,
        Err(err) => {
            tracing::error!(error = %err, "refresh_stale_movies: claim_stale_movies failed");
            return;
        }
    };

    if rows.is_empty() {
        tracing::debug!("refresh_stale_movies: no stale movies");
        return;
    }

    let mut enqueued = 0u32;
    for (movie_id, tmdb_id, source) in &rows {
        let fetch_related = source != "related";
        let payload = serde_json::json!({
            "tmdb_id": tmdb_id,
            "movie_id": movie_id,
            "fetch_related": fetch_related,
        });

        match queries::insert_task(pool, "tmdb_fetch", &payload.to_string()).await {
            Ok(_) => enqueued += 1,
            Err(err) => {
                tracing::warn!(tmdb_id, %err, "refresh_stale_movies: insert_task failed");
            }
        }
    }

    let remaining = queries::count_stale_movies(pool, CURRENT_TMDB_FETCH_VERSION)
        .await
        .unwrap_or(-1);
    tracing::info!(
        enqueued,
        remaining,
        current_version = CURRENT_TMDB_FETCH_VERSION,
        "refresh_stale_movies tick",
    );
}

/// Poll qBittorrent for torrents matching save_path, dedup by dir_name,
/// insert new media_dirs + create tmdb_search tasks, update torrent_info.
pub async fn poll_qbittorrent(
    pool: &SqlitePool,
    config: &crate::config::QbittorrentConfig,
) -> Result<(), String> {
    let client = QbtClient::new(
        &config.base_url,
        &config.username,
        &config.password,
        &config.save_path,
    );

    let torrents = client.fetch_torrents().await?;
    tracing::info!("qBT poll: {} torrents matching save_path", torrents.len());

    let mut new_count = 0;
    let mut update_count = 0;

    for t in &torrents {
        let dir_name = t.dir_name();
        let dir_path = t
            .content_path
            .as_deref()
            .unwrap_or(&t.name);

        // Dedup by dir_name: check if any existing media_dir has the same name
        let existing = queries::find_media_dir_by_name(pool, &dir_name)
            .await
            .map_err(|e| e.to_string())?;

        let media_dir_id = match existing {
            Some(md) => md.id,
            None => {
                // New directory from qBT — insert and create tmdb_search task
                let id = queries::insert_media_dir_with_source(
                    pool, dir_path, &dir_name, "qbittorrent",
                )
                .await
                .map_err(|e| e.to_string())?;

                // Parse dir name and create tmdb_search task
                let parsed = parser::parse_directory_name(&dir_name);
                let payload = serde_json::json!({
                    "dir_id": id,
                    "title": parsed.title,
                    "year": parsed.year,
                    "alt_title": parsed.alt_title,
                });
                let _ = queries::insert_task(pool, "tmdb_search", &payload.to_string()).await;

                new_count += 1;
                id
            }
        };

        // Upsert torrent_info
        queries::upsert_torrent_info(
            pool,
            media_dir_id,
            &t.hash,
            &t.state,
            t.progress,
            t.size.or(t.total_size),
            t.dlspeed,
            t.upspeed,
            t.ratio,
            t.num_seeds,
            t.added_on,
            t.media_type(),
            &t.name,
        )
        .await
        .map_err(|e| e.to_string())?;

        update_count += 1;
    }

    tracing::info!(
        new_dirs = new_count,
        updated_torrents = update_count,
        "qBT poll complete",
    );
    Ok(())
}

pub async fn run_scan_cycle(
    pool: &SqlitePool,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 1) Walk directories (local + remote)
    walker::scan_all_dirs(
        pool,
        &config.scan.movie_dirs,
        config.scan.ssh_key_path.as_deref(),
    )
    .await?;

    // 2) Parse newly discovered directories and create TMDB search tasks
    let new_dirs = queries::get_new_dirs(pool).await?;

    for dir in &new_dirs {
        let parsed = parser::parse_directory_name(&dir.dir_name);
        let payload = serde_json::json!({
            "dir_id": dir.id,
            "title": parsed.title,
            "alt_title": parsed.alt_title,
            "year": parsed.year,
        });

        queries::insert_task(pool, "tmdb_search", &payload.to_string()).await?;
        queries::update_dir_status(pool, dir.id, "parsed").await?;
    }

    tracing::info!(created = new_dirs.len(), "created tmdb_search tasks");
    Ok(())
}

pub async fn process_tmdb_search_tasks(
    pool: &SqlitePool,
    tmdb: &TmdbClient,
    config: &Config,
) {
    let task = match queries::claim_next_task(pool, "tmdb_search").await {
        Ok(Some(task)) => task,
        _ => return,
    };

    let payload: serde_json::Value = match serde_json::from_str(task.payload.as_deref().unwrap_or("{}")) {
        Ok(value) => value,
        Err(err) => {
            if let Err(e) = queries::fail_task(pool, task.id, &err.to_string()).await {
                tracing::warn!(task_id = task.id, error = %e, "fail_task failed");
            }
            return;
        }
    };

    let dir_id = payload["dir_id"].as_i64().unwrap_or(0);
    let title = payload["title"].as_str().unwrap_or("");
    let alt_title = payload["alt_title"].as_str();
    let year = payload["year"].as_u64().map(|y| y as u16);

    tracing::info!(dir_id, title, ?alt_title, year, "processing tmdb_search task");

    // Build the full set of (title, year) queries we'll run against TMDB:
    // parent dir parser output + sidecar evidence collected from inside the
    // dir (inner subdir names + file stems + BDMV META disc title).
    //
    // Two failure modes the parent-only search can't fix:
    //   1. Parent dir name is a Blu-ray volume label like
    //      `BRIDGE_O_T_RIVER_KWAI_UHD_EUR_BLUEBIRD` → parser produces noise,
    //      TMDB returns nothing. But an inner subdir uses proper naming, or
    //      `BDMV/META/DL/bdmt_eng.xml` carries the disc title.
    //   2. Parent dir name has no year, sidecar provides the year.
    //
    // See docs/specs/2026-04-26-sidecar-evidence-design.md.
    let mut search_queries: Vec<(String, Option<u16>)> = Vec::new();
    search_queries.push((title.to_string(), year));
    if let Some(alt) = alt_title.filter(|a| !a.is_empty()) {
        search_queries.push((alt.to_string(), year));
    }

    let dir_path: Option<String> = sqlx::query_scalar(
        "SELECT dir_path FROM media_dirs WHERE id = ?",
    )
    .bind(dir_id)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten();

    if let Some(p) = dir_path.as_deref() {
        let evidence = sidecar::collect_evidence(Path::new(p)).await;
        for cand in evidence.candidates {
            if !cand.title.trim().is_empty() {
                search_queries.push((cand.title.clone(), cand.year));
            }
            if let Some(alt) = cand.alt_title {
                if !alt.trim().is_empty() {
                    search_queries.push((alt, cand.year));
                }
            }
        }
    }

    // Dedupe by lowercase title (year-agnostic so the same title with two
    // different parsed years still gets one TMDB hit; matcher will score
    // year independently).
    let mut seen_titles: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    search_queries.retain(|(t, _)| seen_titles.insert(t.to_lowercase()));

    // Cap at 5 longest titles. Short candidates ("Movie", "Disc 1") are
    // typically noise generated by stems like "movie.mkv"; longer titles
    // carry more signal. Keeping the search budget bounded preserves the
    // existing TMDB rate-limit budget per dir.
    if search_queries.len() > 5 {
        search_queries.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
        search_queries.truncate(5);
    }

    // Run zh-CN + en-US search for each query, with year filter applied.
    let mut all_raw: Vec<crate::tmdb::client::TmdbSearchResult> = Vec::new();
    for (qt, qy) in &search_queries {
        let qy_u32 = qy.map(|y| y as u32);
        let zh = tmdb.search_movie(qt, qy_u32).await.unwrap_or_default();
        let en = tmdb
            .search_movie_with_lang(qt, qy_u32, "en-US")
            .await
            .unwrap_or_default();
        all_raw.extend(zh);
        all_raw.extend(en);
    }

    // Year-less fallback: TMDB's `year=` filter is strict on `primary_release_date`.
    // When a film's TMDB year crosses the dir's stated year (festival vs theatrical
    // vs streaming release), the year-filtered searches above return nothing and
    // the dir gets stamped 'failed' at confidence 0.0 — even though the title
    // alone would have returned the right candidate. Retry without year for every
    // query and let the matcher's year scoring (off ≥ 2 → 0.0) keep us safe from
    // same-name reboots/sequels.
    let any_year = search_queries.iter().any(|(_, y)| y.is_some());
    if all_raw.is_empty() && any_year {
        for (qt, _) in &search_queries {
            let zh = tmdb.search_movie(qt, None).await.unwrap_or_default();
            let en = tmdb
                .search_movie_with_lang(qt, None, "en-US")
                .await
                .unwrap_or_default();
            all_raw.extend(zh);
            all_raw.extend(en);
        }
    }

    if all_raw.is_empty() {
        if let Err(e) = queries::insert_mapping(pool, dir_id, None, "failed", Some(0.0), Some("[]")).await {
            tracing::error!(dir_id, error = %e, "insert_mapping failed (no results)");
            if let Err(e2) = queries::fail_task(pool, task.id, &e.to_string()).await {
                tracing::warn!(task_id = task.id, error = %e2, "fail_task failed");
            }
            return;
        }
        if let Err(e) = queries::update_dir_status(pool, dir_id, "failed").await {
            tracing::warn!(dir_id, error = %e, "update_dir_status failed");
        }
        if let Err(e) = queries::complete_task(pool, task.id).await {
            tracing::warn!(task_id = task.id, error = %e, "complete_task failed");
        }
        return;
    }

    // Deduplicate for storage (keep first occurrence per tmdb_id)
    let mut seen_ids = std::collections::HashSet::new();
    let unique_results: Vec<_> = all_raw.iter()
        .filter(|r| seen_ids.insert(r.id))
        .cloned()
        .collect();
    let candidates_json = serde_json::to_string(&unique_results).unwrap_or_default();

    // Score ALL results against EVERY query (parent + sidecar) and take the
    // best score per tmdb_id. A sidecar query that names the right film will
    // boost the matching candidate above the parent dir's noisy score.
    let mut best_scores: std::collections::HashMap<i64, (f64, crate::tmdb::client::TmdbSearchResult)> = std::collections::HashMap::new();

    for (qt, qy) in &search_queries {
        let scored = score_candidates(qt, *qy, all_raw.clone());
        for sc in scored {
            let entry = best_scores.entry(sc.tmdb_result.id).or_insert((0.0, sc.tmdb_result.clone()));
            if sc.score > entry.0 {
                entry.0 = sc.score;
                entry.1 = sc.tmdb_result;
            }
        }
    }

    // Convert to sorted vec
    let mut scored: Vec<crate::tmdb::matcher::ScoredCandidate> = best_scores.into_values()
        .map(|(score, tmdb_result)| crate::tmdb::matcher::ScoredCandidate { tmdb_result, score })
        .collect();
    scored.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
            let top_score = scored[0].score;
            // Sidecar evidence can pull in candidates from unrelated films (a
            // sample/trailer/extras file with a different movie's name). When
            // top-1 and top-2 are different tmdb_ids and scores are close, we
            // can't safely auto-confirm — downgrade to pending for human review.
            const CONFLICT_GAP: f64 = 0.05;
            let unambiguous = is_unambiguous_winner(&scored, CONFLICT_GAP);
            let raw_decision = decide_match(top_score, config.tmdb.auto_confirm_threshold);
            let decision = if matches!(raw_decision, MatchDecision::AutoConfirm) && !unambiguous {
                tracing::info!(
                    dir_id,
                    top = scored[0].tmdb_result.id,
                    runner_up = scored.get(1).map(|c| c.tmdb_result.id).unwrap_or(0),
                    top_score,
                    "downgrading auto → pending: ambiguous top candidates"
                );
                MatchDecision::Pending
            } else {
                raw_decision
            };

            match decision {
                MatchDecision::AutoConfirm => {
                    let tmdb_id = scored[0].tmdb_result.id;

                    let existing = queries::get_movie_by_tmdb_id(pool, tmdb_id)
                        .await
                        .ok()
                        .flatten();

                    let movie_id = if let Some(movie) = existing {
                        movie.id
                    } else {
                        let title = &scored[0].tmdb_result.title;
                        match queries::insert_movie(
                            pool,
                            tmdb_id,
                            title,
                            scored[0].tmdb_result.original_title.as_deref(),
                            None,
                            scored[0].tmdb_result.overview.as_deref(),
                            scored[0].tmdb_result.poster_path.as_deref(),
                            "[]",
                            None,
                            scored[0].tmdb_result.original_language.as_deref(),
                            None,
                            None,
                            "[]",
                            scored[0].tmdb_result.vote_average,
                            scored[0].tmdb_result.vote_count,
                            "[]",
                            None,
                            None,
                            None,
                            "library",
                        )
                        .await
                        {
                            Ok(id) => id,
                            Err(e) => {
                                tracing::error!(tmdb_id, dir_id, error = %e, "insert_movie failed");
                                if let Err(e2) = queries::fail_task(pool, task.id, &e.to_string()).await {
                                    tracing::warn!(task_id = task.id, error = %e2, "fail_task failed");
                                }
                                return;
                            }
                        }
                    };

                    if let Err(e) = queries::insert_mapping(
                        pool,
                        dir_id,
                        Some(movie_id),
                        "auto",
                        Some(top_score),
                        Some(&candidates_json),
                    )
                    .await
                    {
                        tracing::error!(dir_id, error = %e, "insert_mapping failed (auto)");
                        if let Err(e2) = queries::fail_task(pool, task.id, &e.to_string()).await {
                            tracing::warn!(task_id = task.id, error = %e2, "fail_task failed");
                        }
                        return;
                    }
                    if let Err(e) = queries::update_dir_status(pool, dir_id, "matched").await {
                        tracing::warn!(dir_id, error = %e, "update_dir_status failed");
                    }

                    let fetch_payload = serde_json::json!({
                        "tmdb_id": tmdb_id,
                        "movie_id": movie_id,
                        "fetch_related": true,
                    });
                    if let Err(e) = queries::insert_task(pool, "tmdb_fetch", &fetch_payload.to_string()).await {
                        tracing::error!(dir_id, tmdb_id, error = %e, "insert_task(tmdb_fetch) failed");
                    }
                }
                MatchDecision::Pending => {
                    if let Err(e) = queries::insert_mapping(
                        pool,
                        dir_id,
                        None,
                        "pending",
                        Some(top_score),
                        Some(&candidates_json),
                    )
                    .await
                    {
                        tracing::error!(dir_id, error = %e, "insert_mapping failed (pending)");
                        if let Err(e2) = queries::fail_task(pool, task.id, &e.to_string()).await {
                            tracing::warn!(task_id = task.id, error = %e2, "fail_task failed");
                        }
                        return;
                    }
                    if let Err(e) = queries::update_dir_status(pool, dir_id, "parsed").await {
                        tracing::warn!(dir_id, error = %e, "update_dir_status failed");
                    }
                }
                MatchDecision::Failed => {
                    if let Err(e) = queries::insert_mapping(
                        pool,
                        dir_id,
                        None,
                        "failed",
                        Some(top_score),
                        Some(&candidates_json),
                    )
                    .await
                    {
                        tracing::error!(dir_id, error = %e, "insert_mapping failed (match failed)");
                        if let Err(e2) = queries::fail_task(pool, task.id, &e.to_string()).await {
                            tracing::warn!(task_id = task.id, error = %e2, "fail_task failed");
                        }
                        return;
                    }
                    if let Err(e) = queries::update_dir_status(pool, dir_id, "failed").await {
                        tracing::warn!(dir_id, error = %e, "update_dir_status failed");
                    }
                }
            }

            if let Err(e) = queries::complete_task(pool, task.id).await {
                tracing::warn!(task_id = task.id, error = %e, "complete_task failed");
            }
}

// --- MovieFields: all columns for UPDATE movies ---
struct MovieFields {
    title: String,
    original_title: Option<String>,
    year: Option<i64>,
    overview: Option<String>,
    poster_url: Option<String>,
    genres: String,
    country: Option<String>,
    language: Option<String>,
    runtime: Option<i64>,
    director_name: Option<String>,
    director_info: String,
    cast: String,
    keywords: String,
    tmdb_rating: Option<f64>,
    tmdb_votes: Option<i64>,
    budget: Option<i64>,
    revenue: Option<i64>,
    popularity: Option<f64>,
    title_en: Option<String>,
    overview_en: Option<String>,
    tagline_zh: Option<String>,
    tagline_en: Option<String>,
    genres_zh: String,
    genres_en: String,
    imdb_id: Option<String>,
    backdrop_path: Option<String>,
    homepage: Option<String>,
    status: Option<String>,
    collection: Option<String>,
    production_companies: Option<String>,
    spoken_languages: Option<String>,
    origin_country: Option<String>,
    director_info_en: Option<String>,
    cast_en: Option<String>,
    keywords_en: Option<String>,
    collection_en: Option<String>,
    production_companies_en: Option<String>,
    /// When true, `update_movie_record` will preserve the existing
    /// `overview` and `overview_zh_source` fields (used to keep an LLM
    /// translation that's better than the new TMDB response).
    skip_overview_update: bool,
    /// New value for `movies.overview_zh_source` when not skipping. None
    /// when overview_en alone changed.
    overview_zh_source: Option<String>,
}

/// Decide whether the new TMDB zh-CN overview should overwrite the existing
/// row. Refuses to overwrite an LLM-sourced translation with an empty or
/// significantly shorter TMDB payload — TMDB stubs are common and clobbering
/// a richer LLM translation hurts the embedding text.
///
/// Returns `(skip_overview_update, overview_zh_source)`:
/// - `skip_overview_update = true` → leave overview / overview_zh_source as-is
/// - else → overwrite overview with new_zh, set source to the returned value
///
/// The 50% length threshold is a starting point; real-world hit rate gets
/// reviewed once deployed (logs `preserving LLM zh overview ...`).
fn resolve_overview_update(
    new_zh: &str,
    current: Option<&queries::OverviewState>,
) -> (bool, Option<String>) {
    let current_state = match current {
        Some(s) => s,
        None => {
            // No existing row state (first-time write or unknown). Always
            // accept whatever TMDB gave us — even if empty.
            return (false, if new_zh.is_empty() { None } else { Some("tmdb".to_string()) });
        }
    };

    let is_llm = current_state.overview_zh_source.as_deref() == Some("llm");
    let current_text = current_state.overview.as_deref().unwrap_or("");
    let current_chars = current_text.chars().count();
    let new_chars = new_zh.chars().count();

    if is_llm {
        // Keep LLM translation when TMDB returned nothing.
        if new_zh.is_empty() {
            tracing::info!(
                current_len = current_chars,
                "preserving LLM zh overview (TMDB returned empty)"
            );
            return (true, None);
        }
        // Keep LLM translation when TMDB returned a much shorter stub.
        if current_chars > 0 && new_chars * 2 < current_chars {
            tracing::info!(
                current_len = current_chars,
                new_len = new_chars,
                "preserving LLM zh overview (TMDB shorter than 50% of LLM)"
            );
            return (true, None);
        }
    }

    // Either non-LLM source, or TMDB returned a credible payload — accept it.
    if new_zh.is_empty() {
        // Don't store empty source attribution; leave column at NULL/previous.
        (false, None)
    } else {
        (false, Some("tmdb".to_string()))
    }
}

fn build_movie_fields(
    full: &crate::tmdb::client::TmdbMovieFull,
    en: Option<&crate::tmdb::client::TmdbMovieFull>,
) -> MovieFields {
    let year = full
        .release_date
        .as_ref()
        .and_then(|d| d.get(..4))
        .and_then(|y| y.parse::<i64>().ok());

    let genres_zh_vec: Vec<String> = full
        .genres
        .as_ref()
        .map(|g| g.iter().map(|x| x.name.clone()).collect())
        .unwrap_or_default();
    let genres_en_vec: Vec<String> = en
        .and_then(|e| e.genres.as_ref())
        .map(|g| g.iter().map(|x| x.name.clone()).collect())
        .unwrap_or_default();

    let country = full
        .production_countries
        .as_ref()
        .and_then(|c| c.first())
        .map(|c| c.iso_3166_1.clone());

    let credits = full.credits.as_ref();
    let directors: Vec<serde_json::Value> = credits
        .and_then(|c| c.crew.as_ref())
        .map(|crew| {
            crew.iter()
                .filter(|m| m.job == "Director")
                .map(|d| {
                    serde_json::json!({"name": d.name, "tmdb_person_id": d.id, "profile_path": d.profile_path})
                })
                .collect()
        })
        .unwrap_or_default();
    let director_name = directors
        .first()
        .and_then(|d| d["name"].as_str())
        .map(|s| s.to_string());

    let cast_structured: Vec<serde_json::Value> = credits
        .and_then(|c| c.cast.as_ref())
        .map(|cast| {
            cast.iter()
                .take(8)
                .map(|a| {
                    serde_json::json!({"name": a.name, "tmdb_person_id": a.id, "character": a.character, "profile_path": a.profile_path})
                })
                .collect()
        })
        .unwrap_or_default();

    let keywords_list: Vec<String> = full
        .keywords
        .as_ref()
        .and_then(|k| k.keywords.as_ref())
        .map(|kw| kw.iter().map(|k| k.name.clone()).collect())
        .unwrap_or_default();

    // English-language equivalents from the parallel en-US full fetch.
    let en_credits = en.and_then(|e| e.credits.as_ref());
    let en_directors: Vec<serde_json::Value> = en_credits
        .and_then(|c| c.crew.as_ref())
        .map(|crew| {
            crew.iter()
                .filter(|m| m.job == "Director")
                .map(|d| {
                    serde_json::json!({"name": d.name, "tmdb_person_id": d.id, "profile_path": d.profile_path})
                })
                .collect()
        })
        .unwrap_or_default();
    let en_cast_structured: Vec<serde_json::Value> = en_credits
        .and_then(|c| c.cast.as_ref())
        .map(|cast| {
            cast.iter()
                .take(8)
                .map(|a| {
                    serde_json::json!({"name": a.name, "tmdb_person_id": a.id, "character": a.character, "profile_path": a.profile_path})
                })
                .collect()
        })
        .unwrap_or_default();
    let en_keywords_list: Vec<String> = en
        .and_then(|e| e.keywords.as_ref())
        .and_then(|k| k.keywords.as_ref())
        .map(|kw| kw.iter().map(|k| k.name.clone()).collect())
        .unwrap_or_default();

    let poster_url = select_poster_url(full);

    MovieFields {
        title: full.title.clone(),
        original_title: full.original_title.clone(),
        year,
        overview: full.overview.clone(),
        poster_url,
        genres: serde_json::to_string(&genres_zh_vec).unwrap_or_else(|_| "[]".to_string()),
        country,
        language: full.original_language.clone(),
        runtime: full.runtime,
        director_name,
        director_info: serde_json::to_string(&directors).unwrap_or_else(|_| "[]".to_string()),
        cast: serde_json::to_string(&cast_structured).unwrap_or_else(|_| "[]".to_string()),
        keywords: serde_json::to_string(&keywords_list).unwrap_or_else(|_| "[]".to_string()),
        tmdb_rating: full.vote_average,
        tmdb_votes: full.vote_count,
        budget: full.budget,
        revenue: full.revenue,
        popularity: full.popularity,
        title_en: en.map(|e| e.title.clone()),
        overview_en: en.and_then(|e| e.overview.clone()),
        tagline_zh: full.tagline.clone(),
        tagline_en: en.and_then(|e| e.tagline.clone()),
        genres_zh: serde_json::to_string(&genres_zh_vec).unwrap_or_else(|_| "[]".to_string()),
        genres_en: serde_json::to_string(&genres_en_vec).unwrap_or_else(|_| "[]".to_string()),
        imdb_id: full.imdb_id.clone(),
        backdrop_path: full.backdrop_path.clone(),
        homepage: full.homepage.clone(),
        status: full.status.clone(),
        collection: full
            .belongs_to_collection
            .as_ref()
            .map(|c| serde_json::to_string(c).unwrap_or_default()),
        production_companies: full
            .production_companies
            .as_ref()
            .map(|c| serde_json::to_string(c).unwrap_or_default()),
        spoken_languages: full
            .spoken_languages
            .as_ref()
            .map(|l| serde_json::to_string(l).unwrap_or_default()),
        origin_country: full
            .origin_country
            .as_ref()
            .map(|o| serde_json::to_string(o).unwrap_or_default()),
        director_info_en: en.map(|_| serde_json::to_string(&en_directors).unwrap_or_else(|_| "[]".to_string())),
        cast_en: en.map(|_| serde_json::to_string(&en_cast_structured).unwrap_or_else(|_| "[]".to_string())),
        keywords_en: en.map(|_| serde_json::to_string(&en_keywords_list).unwrap_or_else(|_| "[]".to_string())),
        collection_en: en
            .and_then(|e| e.belongs_to_collection.as_ref())
            .map(|c| serde_json::to_string(c).unwrap_or_default()),
        production_companies_en: en
            .and_then(|e| e.production_companies.as_ref())
            .map(|c| serde_json::to_string(c).unwrap_or_default()),
        // Filled in by the caller after consulting current overview state.
        skip_overview_update: false,
        overview_zh_source: None,
    }
}

/// Anything that can stand in as a poster candidate for `pick_best_poster_path`.
/// Implemented for both TMDB API responses (`TmdbImage`) and DB rows
/// (`MovieImage`) so the same selection algorithm is shared by the fetch worker
/// and the admin "repick from local DB" backfill.
pub trait PosterEntry {
    fn iso_639_1(&self) -> Option<&str>;
    fn vote_average(&self) -> f64;
    fn file_path(&self) -> &str;
}

impl PosterEntry for crate::tmdb::client::TmdbImage {
    fn iso_639_1(&self) -> Option<&str> { self.iso_639_1.as_deref() }
    fn vote_average(&self) -> f64 { self.vote_average.unwrap_or(0.0) }
    fn file_path(&self) -> &str { &self.file_path }
}

fn best_in_lang<'a, P: PosterEntry>(posters: &'a [P], lang: &str) -> Option<&'a P> {
    posters
        .iter()
        .filter(|p| p.iso_639_1() == Some(lang))
        .max_by(|a, b| {
            a.vote_average()
                .partial_cmp(&b.vote_average())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
}

fn best_overall<P: PosterEntry>(posters: &[P]) -> Option<&P> {
    posters.iter().max_by(|a, b| {
        a.vote_average()
            .partial_cmp(&b.vote_average())
            .unwrap_or(std::cmp::Ordering::Equal)
    })
}

/// Pick the best poster file path: `original_language → en → any-best`.
///
/// Why fallback to en (not pure best score): when a film has no poster in its
/// original language, "highest-rated regardless of language" can land on a
/// Chinese / Japanese / etc. localized poster that surprises the user. English
/// is the de-facto international fallback even on TMDB. Plex / Jellyfin follow
/// the same precedence.
///
/// Returns the bare TMDB file_path (e.g. "/abc.jpg"); caller prepends the CDN.
pub fn pick_best_poster_path<'a, P: PosterEntry>(
    orig_lang: &str,
    posters: &'a [P],
) -> Option<&'a str> {
    best_in_lang(posters, orig_lang)
        .or_else(|| {
            if orig_lang != "en" {
                best_in_lang(posters, "en")
            } else {
                None
            }
        })
        .or_else(|| best_overall(posters))
        .map(|p| p.file_path())
}

fn select_poster_url(full: &crate::tmdb::client::TmdbMovieFull) -> Option<String> {
    full.images
        .as_ref()
        .and_then(|images| {
            let orig_lang = full.original_language.as_deref().unwrap_or("en");
            images
                .posters
                .as_ref()
                .and_then(|posters| pick_best_poster_path(orig_lang, posters))
                .map(|p| format!("{}{}", TMDB_IMG_BASE, p))
        })
        .or_else(|| {
            full.poster_path
                .as_ref()
                .map(|p| format!("{}{}", TMDB_IMG_BASE, p))
        })
}

fn build_credit_rows(
    zh_credits: Option<&crate::tmdb::client::TmdbCreditsResponse>,
    en_credits: Option<&crate::tmdb::client::TmdbCreditsResponse>,
) -> Vec<queries::CreditRow> {
    let mut rows = Vec::new();
    if let Some(credits) = zh_credits {
        if let Some(cast) = &credits.cast {
            for c in cast {
                // TMDB cast `character` is localized; match en cast by person id.
                let en_match = en_credits
                    .and_then(|ec| ec.cast.as_ref())
                    .and_then(|cc| cc.iter().find(|e| e.id == c.id));
                rows.push(queries::CreditRow {
                    tmdb_person_id: c.id,
                    person_name: c.name.clone(),
                    credit_type: "cast".to_string(),
                    role: c.character.clone(),
                    department: None,
                    order: c.order,
                    profile_path: c.profile_path.clone(),
                    person_name_en: en_match.map(|m| m.name.clone()),
                    role_en: en_match.and_then(|m| m.character.clone()),
                });
            }
        }
        if let Some(crew) = &credits.crew {
            for c in crew {
                // Crew `job` is language-invariant in TMDB; disambiguate by (id, job).
                let en_match = en_credits
                    .and_then(|ec| ec.crew.as_ref())
                    .and_then(|cc| cc.iter().find(|e| e.id == c.id && e.job == c.job));
                rows.push(queries::CreditRow {
                    tmdb_person_id: c.id,
                    person_name: c.name.clone(),
                    credit_type: "crew".to_string(),
                    role: Some(c.job.clone()),
                    department: None,
                    order: None,
                    profile_path: c.profile_path.clone(),
                    person_name_en: en_match.map(|m| m.name.clone()),
                    role_en: Some(c.job.clone()),
                });
            }
        }
    }
    rows
}

fn build_image_rows(images: Option<&crate::tmdb::client::TmdbFullImagesResponse>) -> Vec<queries::ImageRow> {
    let mut rows = Vec::new();
    if let Some(images) = images {
        let configs = [
            ("poster", &images.posters),
            ("backdrop", &images.backdrops),
            ("logo", &images.logos),
        ];
        for (image_type, list) in configs {
            if let Some(items) = list {
                for img in items {
                    rows.push(queries::ImageRow {
                        image_type: image_type.to_string(),
                        file_path: img.file_path.clone(),
                        iso_639_1: img.iso_639_1.clone(),
                        width: img.width,
                        height: img.height,
                        vote_average: img.vote_average,
                    });
                }
            }
        }
    }
    rows
}

fn build_video_rows(full: &crate::tmdb::client::TmdbMovieFull) -> Vec<queries::VideoRow> {
    let mut rows = Vec::new();
    if let Some(videos) = &full.videos {
        if let Some(list) = &videos.results {
            for v in list {
                rows.push(queries::VideoRow {
                    video_key: v.key.clone(),
                    site: v.site.clone(),
                    video_type: v.video_type.clone(),
                    name: v.name.clone(),
                    iso_639_1: v.iso_639_1.clone(),
                    official: v.official.unwrap_or(false),
                    published_at: v.published_at.clone(),
                });
            }
        }
    }
    rows
}

fn build_review_rows(full: &crate::tmdb::client::TmdbMovieFull) -> Vec<queries::ReviewRow> {
    let mut rows = Vec::new();
    if let Some(reviews) = &full.reviews {
        if let Some(list) = &reviews.results {
            for r in list {
                rows.push(queries::ReviewRow {
                    tmdb_review_id: r.id.clone(),
                    author: r.author.clone(),
                    author_username: r
                        .author_details
                        .as_ref()
                        .and_then(|d| d.username.clone()),
                    content: r.content.clone(),
                    rating: r.author_details.as_ref().and_then(|d| d.rating),
                    created_at: r.created_at.clone(),
                    updated_at: r.updated_at.clone(),
                });
            }
        }
    }
    rows
}

fn build_release_date_rows(full: &crate::tmdb::client::TmdbMovieFull) -> Vec<queries::ReleaseDateRow> {
    let mut rows = Vec::new();
    if let Some(releases) = &full.release_dates {
        if let Some(countries) = &releases.results {
            for c in countries {
                for entry in &c.release_dates {
                    rows.push(queries::ReleaseDateRow {
                        iso_3166_1: c.iso_3166_1.clone(),
                        release_date: entry.release_date.clone(),
                        certification: entry.certification.clone(),
                        release_type: entry.release_type,
                        note: entry.note.clone(),
                    });
                }
            }
        }
    }
    rows
}

fn build_watch_provider_rows(full: &crate::tmdb::client::TmdbMovieFull) -> Vec<queries::WatchProviderRow> {
    let mut rows = Vec::new();
    if let Some(wp) = &full.watch_providers {
        if let Some(results) = &wp.results {
            for (country_code, providers) in results {
                let variants = [
                    ("flatrate", &providers.flatrate),
                    ("rent", &providers.rent),
                    ("buy", &providers.buy),
                    ("ads", &providers.ads),
                ];
                for (ptype, items) in variants {
                    if let Some(items) = items {
                        for item in items {
                            rows.push(queries::WatchProviderRow {
                                iso_3166_1: country_code.clone(),
                                provider_id: item.provider_id,
                                provider_name: item.provider_name.clone(),
                                logo_path: item.logo_path.clone(),
                                provider_type: ptype.to_string(),
                                display_priority: item.display_priority,
                            });
                        }
                    }
                }
            }
        }
    }
    rows
}

fn build_external_id_row(full: &crate::tmdb::client::TmdbMovieFull) -> queries::ExternalIdRow {
    let ext = full.external_ids.as_ref();
    queries::ExternalIdRow {
        imdb_id: ext.and_then(|e| e.imdb_id.clone()),
        facebook_id: ext.and_then(|e| e.facebook_id.clone()),
        instagram_id: ext.and_then(|e| e.instagram_id.clone()),
        twitter_id: ext.and_then(|e| e.twitter_id.clone()),
        wikidata_id: ext.and_then(|e| e.wikidata_id.clone()),
    }
}

fn build_alternative_title_rows(full: &crate::tmdb::client::TmdbMovieFull) -> Vec<queries::AlternativeTitleRow> {
    let mut rows = Vec::new();
    if let Some(alts) = &full.alternative_titles {
        if let Some(titles) = &alts.titles {
            for t in titles {
                rows.push(queries::AlternativeTitleRow {
                    iso_3166_1: t.iso_3166_1.clone(),
                    title: t.title.clone(),
                    title_type: t.title_type.clone(),
                });
            }
        }
    }
    rows
}

fn build_translation_rows(full: &crate::tmdb::client::TmdbMovieFull) -> Vec<queries::TranslationRow> {
    let mut rows = Vec::new();
    if let Some(trans) = &full.translations {
        if let Some(items) = &trans.translations {
            for t in items {
                let data = t.data.as_ref();
                rows.push(queries::TranslationRow {
                    iso_639_1: t.iso_639_1.clone(),
                    iso_3166_1: t.iso_3166_1.clone(),
                    language_name: t.name.clone().or_else(|| t.english_name.clone()),
                    title: data.and_then(|d| d.title.clone()),
                    overview: data.and_then(|d| d.overview.clone()),
                    tagline: data.and_then(|d| d.tagline.clone()),
                    homepage: data.and_then(|d| d.homepage.clone()),
                    runtime: data.and_then(|d| d.runtime),
                });
            }
        }
    }
    rows
}

fn build_list_rows(full: &crate::tmdb::client::TmdbMovieFull) -> Vec<queries::MovieListRow> {
    let mut rows = Vec::new();
    if let Some(lists) = &full.lists {
        if let Some(results) = &lists.results {
            for l in results {
                rows.push(queries::MovieListRow {
                    tmdb_list_id: l.id,
                    list_name: l.name.clone(),
                    description: l.description.clone(),
                    item_count: l.item_count,
                    iso_639_1: l.iso_639_1.clone(),
                });
            }
        }
    }
    rows
}

async fn process_related_movies(
    pool: &SqlitePool,
    movie_id: i64,
    full: &crate::tmdb::client::TmdbMovieFull,
) {
    let mut related_rows = Vec::new();
    let related_sources = [
        ("similar", full.similar.as_ref().and_then(|s| s.results.as_ref())),
        (
            "recommendation",
            full.recommendations.as_ref().and_then(|r| r.results.as_ref()),
        ),
    ];

    for (relation, list) in related_sources {
        if let Some(movies) = list {
            for m in movies {
                related_rows.push(queries::RelatedMovieRow {
                    related_tmdb_id: m.id,
                    relation_type: relation.to_string(),
                });

                let exists = queries::get_movie_by_tmdb_id(pool, m.id)
                    .await
                    .ok()
                    .flatten();

                if exists.is_none() {
                    let new_id = match queries::insert_movie(
                        pool,
                        m.id,
                        &m.title,
                        m.original_title.as_deref(),
                        None,
                        m.overview.as_deref(),
                        m.poster_path.as_deref(),
                        "[]",
                        None,
                        None,
                        None,
                        None,
                        "[]",
                        m.vote_average,
                        m.vote_count,
                        "[]",
                        None,
                        None,
                        None,
                        "related",
                    )
                    .await
                    {
                        Ok(id) => id,
                        Err(e) => {
                            tracing::warn!(tmdb_id = m.id, error = %e, "insert_movie failed (related)");
                            continue;
                        }
                    };

                    if new_id > 0 {
                        let fetch_payload = serde_json::json!({
                            "tmdb_id": m.id,
                            "movie_id": new_id,
                            "fetch_related": false,
                        });
                        if let Err(e) = queries::insert_task(
                            pool,
                            "tmdb_fetch",
                            &fetch_payload.to_string(),
                        )
                        .await
                        {
                            tracing::warn!(tmdb_id = m.id, error = %e, "insert_task(tmdb_fetch) failed (related)");
                        }
                    }
                }
            }
        }
    }

    if let Err(e) = queries::replace_related_movies(pool, movie_id, &related_rows).await {
        tracing::warn!(movie_id, error = %e, "replace_related_movies failed");
    }
}

async fn update_movie_record(
    pool: &SqlitePool,
    tmdb_id: i64,
    f: &MovieFields,
) -> Result<(), sqlx::Error> {
    // The `overview` / `overview_zh` / `overview_zh_source` triplet is updated
    // conditionally — when `skip_overview_update` is set, we keep the existing
    // LLM translation rather than clobbering it with a shorter TMDB stub.
    // Splitting into two SQL statements keeps the bind list straightforward;
    // overview_en is always written because TMDB-en is authoritative for English.
    let common_sql_head = "UPDATE movies SET
        title = ?, original_title = ?, year = ?,
        poster_url = ?, genres = ?, country = ?, language = ?,
        runtime = ?, director = ?, director_info = ?, cast = ?, keywords = ?,
        tmdb_rating = ?, tmdb_votes = ?,
        budget = ?, revenue = ?, popularity = ?,
        title_zh = ?, title_en = ?, overview_en = ?,
        tagline_zh = ?, tagline_en = ?, genres_zh = ?, genres_en = ?,
        imdb_id = ?, backdrop_path = ?, homepage = ?, status = ?,
        collection = ?, production_companies = ?, spoken_languages = ?, origin_country = ?,
        director_info_en = ?, cast_en = ?, keywords_en = ?,
        collection_en = ?, production_companies_en = ?,";

    let sql = if f.skip_overview_update {
        format!(
            "{} updated_at = datetime('now') WHERE tmdb_id = ?",
            common_sql_head
        )
    } else {
        format!(
            "{} overview = ?, overview_zh = ?, overview_zh_source = ?, updated_at = datetime('now') WHERE tmdb_id = ?",
            common_sql_head
        )
    };

    let mut q = sqlx::query(&sql)
        .bind(&f.title)
        .bind(&f.original_title)
        .bind(f.year)
        .bind(&f.poster_url)
        .bind(&f.genres)
        .bind(&f.country)
        .bind(&f.language)
        .bind(f.runtime)
        .bind(&f.director_name)
        .bind(&f.director_info)
        .bind(&f.cast)
        .bind(&f.keywords)
        .bind(f.tmdb_rating)
        .bind(f.tmdb_votes)
        .bind(f.budget)
        .bind(f.revenue)
        .bind(f.popularity)
        .bind(&f.title)
        .bind(&f.title_en)
        .bind(&f.overview_en)
        .bind(&f.tagline_zh)
        .bind(&f.tagline_en)
        .bind(&f.genres_zh)
        .bind(&f.genres_en)
        .bind(&f.imdb_id)
        .bind(&f.backdrop_path)
        .bind(&f.homepage)
        .bind(&f.status)
        .bind(&f.collection)
        .bind(&f.production_companies)
        .bind(&f.spoken_languages)
        .bind(&f.origin_country)
        .bind(&f.director_info_en)
        .bind(&f.cast_en)
        .bind(&f.keywords_en)
        .bind(&f.collection_en)
        .bind(&f.production_companies_en);

    if !f.skip_overview_update {
        q = q
            .bind(&f.overview)
            .bind(&f.overview)
            .bind(&f.overview_zh_source);
    }

    q.bind(tmdb_id).execute(pool).await?;
    Ok(())
}

// Macro to reduce boilerplate for non-critical sub-resource writes
macro_rules! write_sub_resource {
    ($pool:expr, $movie_id:expr, $name:expr, $func:expr, $rows:expr) => {
        if let Err(e) = $func($pool, $movie_id, $rows).await {
            tracing::warn!(movie_id = $movie_id, error = %e, concat!($name, " failed"));
        }
    };
}

pub async fn process_tmdb_fetch_tasks(pool: &SqlitePool, tmdb: &TmdbClient) {
    let task = match queries::claim_next_task(pool, "tmdb_fetch").await {
        Ok(Some(task)) => task,
        _ => return,
    };

    let payload: serde_json::Value = match serde_json::from_str(task.payload.as_deref().unwrap_or("{}")) {
        Ok(value) => value,
        Err(err) => {
            if let Err(e) = queries::fail_task(pool, task.id, &err.to_string()).await {
                tracing::warn!(task_id = task.id, error = %e, "fail_task failed");
            }
            return;
        }
    };

    let tmdb_id = payload["tmdb_id"].as_i64().unwrap_or(0);
    let movie_id_from_payload = payload["movie_id"].as_i64();
    let fetch_related = payload["fetch_related"].as_bool().unwrap_or(true);

    tracing::info!(tmdb_id, fetch_related, "processing tmdb_fetch task");

    let (full_res, en_res) = tokio::join!(
        tmdb.get_movie_full(tmdb_id, "zh-CN"),
        tmdb.get_movie_full_minimal(tmdb_id, "en-US"),
    );

    let full = match full_res {
        Ok(full) => full,
        Err(err) => {
            tracing::error!(tmdb_id, error = %err, "tmdb full fetch failed");
            if let Err(e) = queries::fail_task(pool, task.id, &err.to_string()).await {
                tracing::warn!(task_id = task.id, error = %e, "fail_task failed");
            }
            return;
        }
    };
    let en = en_res.ok();

    // Build fields and update movie record
    let mut fields = build_movie_fields(&full, en.as_ref());

    // Decide whether the new TMDB zh-CN overview should overwrite a possibly
    // LLM-translated existing one. See `resolve_overview_update` doc comment.
    let current_overview_state = queries::get_movie_overview_state(pool, tmdb_id)
        .await
        .ok()
        .flatten();
    let new_zh = fields.overview.clone().unwrap_or_default();
    let (skip_overview, new_source) =
        resolve_overview_update(&new_zh, current_overview_state.as_ref());
    fields.skip_overview_update = skip_overview;
    fields.overview_zh_source = new_source;

    if let Err(err) = update_movie_record(pool, tmdb_id, &fields).await {
        tracing::error!(tmdb_id, error = %err, "failed to update movie");
        if let Err(e) = queries::fail_task(pool, task.id, &err.to_string()).await {
            tracing::warn!(task_id = task.id, error = %e, "fail_task failed");
        }
        return;
    }

    // Seed translation queue with any new English keywords this movie brought
    // in. Idempotent INSERT OR IGNORE — already-known keywords are no-ops, new
    // ones land in `pending` for the translation worker to pick up.
    let kw_list: Vec<String> = serde_json::from_str(&fields.keywords).unwrap_or_default();
    if !kw_list.is_empty() {
        if let Err(e) = queries::ensure_keyword_translation_rows(pool, &kw_list).await {
            tracing::warn!(tmdb_id, error = %e, "ensure_keyword_translation_rows failed");
        }
    }

    // Resolve movie_id
    let movie_id = match movie_id_from_payload {
        Some(id) => id,
        None => queries::get_movie_by_tmdb_id(pool, tmdb_id)
            .await
            .ok()
            .flatten()
            .map(|m| m.id)
            .unwrap_or(0),
    };
    if movie_id == 0 {
        tracing::error!(tmdb_id, "movie not found after update");
        if let Err(e) = queries::complete_task(pool, task.id).await {
            tracing::warn!(task_id = task.id, error = %e, "complete_task failed");
        }
        return;
    }

    // Write sub-resources (non-critical — warn on failure)
    write_sub_resource!(pool, movie_id, "replace_movie_credits", queries::replace_movie_credits, &build_credit_rows(full.credits.as_ref(), en.as_ref().and_then(|e| e.credits.as_ref())));
    write_sub_resource!(pool, movie_id, "replace_movie_images", queries::replace_movie_images, &build_image_rows(full.images.as_ref()));
    write_sub_resource!(pool, movie_id, "replace_movie_videos", queries::replace_movie_videos, &build_video_rows(&full));
    write_sub_resource!(pool, movie_id, "replace_movie_reviews", queries::replace_movie_reviews, &build_review_rows(&full));
    write_sub_resource!(pool, movie_id, "replace_movie_release_dates", queries::replace_movie_release_dates, &build_release_date_rows(&full));
    write_sub_resource!(pool, movie_id, "replace_movie_watch_providers", queries::replace_movie_watch_providers, &build_watch_provider_rows(&full));
    if let Err(e) = queries::replace_movie_external_ids(pool, movie_id, &build_external_id_row(&full)).await {
        tracing::warn!(movie_id, error = %e, "replace_movie_external_ids failed");
    }
    write_sub_resource!(pool, movie_id, "replace_movie_alternative_titles", queries::replace_movie_alternative_titles, &build_alternative_title_rows(&full));
    write_sub_resource!(pool, movie_id, "replace_movie_translations", queries::replace_movie_translations, &build_translation_rows(&full));
    write_sub_resource!(pool, movie_id, "replace_movie_lists", queries::replace_movie_lists, &build_list_rows(&full));

    // Related movies
    if fetch_related {
        process_related_movies(pool, movie_id, &full).await;
    }

    // 本次成功走完 pipeline，打上当前版本号。refresh_stale_movies_worker 下一轮
    // 就不会再把这行选出来。sub-resource 写入可能有 warn，但主记录和 related
    // 都已落库，版本标记在这里是合适的。
    if let Err(e) = queries::set_movie_fetch_version(pool, tmdb_id, CURRENT_TMDB_FETCH_VERSION).await
    {
        tracing::warn!(tmdb_id, error = %e, "set_movie_fetch_version failed");
    }

    if let Err(e) = queries::complete_task(pool, task.id).await {
        tracing::warn!(task_id = task.id, error = %e, "complete_task failed");
    }
}

#[cfg(test)]
mod resolve_overview_tests {
    use super::*;

    fn state(overview: Option<&str>, source: Option<&str>) -> queries::OverviewState {
        queries::OverviewState {
            overview: overview.map(String::from),
            overview_zh_source: source.map(String::from),
        }
    }

    #[test]
    fn no_current_state_accepts_new_zh() {
        let (skip, src) = resolve_overview_update("新中文剧情", None);
        assert!(!skip);
        assert_eq!(src.as_deref(), Some("tmdb"));
    }

    #[test]
    fn no_current_state_accepts_empty() {
        let (skip, src) = resolve_overview_update("", None);
        assert!(!skip);
        assert!(src.is_none());
    }

    #[test]
    fn llm_preserved_when_tmdb_empty() {
        let s = state(Some("LLM 的完整中文剧情翻译，描述了主角骑摩托的旅程"), Some("llm"));
        let (skip, src) = resolve_overview_update("", Some(&s));
        assert!(skip, "should preserve LLM when TMDB returns empty");
        assert!(src.is_none());
    }

    #[test]
    fn llm_preserved_when_tmdb_significantly_shorter() {
        // LLM 30 chars vs TMDB 10 chars (10*2=20 < 30) → preserve
        let s = state(Some("一二三四五六七八九十一二三四五六七八九十一二三四五六七八九十"), Some("llm"));
        let (skip, src) = resolve_overview_update("一二三四五六七八九十", Some(&s));
        assert!(skip, "should preserve LLM when TMDB is < 50% length");
        assert!(src.is_none());
    }

    #[test]
    fn llm_overwritten_when_tmdb_comparable_length() {
        // LLM 20 chars vs TMDB 12 chars (12*2=24 >= 20) → overwrite
        let s = state(Some("一二三四五六七八九十一二三四五六七八九十"), Some("llm"));
        let (skip, src) = resolve_overview_update("一二三四五六七八九十一二", Some(&s));
        assert!(!skip);
        assert_eq!(src.as_deref(), Some("tmdb"));
    }

    #[test]
    fn tmdb_source_always_overwritten_by_new_tmdb() {
        // current source = 'tmdb' should not be preserved even if shorter
        let s = state(Some("旧的较长 TMDB 中文剧情，描述详细"), Some("tmdb"));
        let (skip, src) = resolve_overview_update("新的短 TMDB 描述", Some(&s));
        assert!(!skip);
        assert_eq!(src.as_deref(), Some("tmdb"));
    }

    #[test]
    fn null_source_treated_as_overwritable() {
        // overview_zh_source = NULL — treat as "no LLM protection" → accept new
        let s = state(Some("某些遗留 zh 内容"), None);
        let (skip, src) = resolve_overview_update("更短的 TMDB 描述", Some(&s));
        assert!(!skip);
        assert_eq!(src.as_deref(), Some("tmdb"));
    }

    #[test]
    fn empty_new_zh_against_tmdb_source_skips_overwrite_without_attribution() {
        // current source = 'tmdb' or NULL, new is empty: don't write empty + don't change source
        let s = state(Some("现存 TMDB 内容"), Some("tmdb"));
        let (skip, src) = resolve_overview_update("", Some(&s));
        assert!(!skip);
        assert!(src.is_none());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        AuthConfig, Config, DatabaseConfig, LlmConfig, ScanConfig, ServerConfig, TmdbConfig,
    };
    use sqlx::SqlitePool;
    use std::fs;
    use std::path::Path;
    use tempfile::TempDir;

    fn test_config(movie_dir: &Path) -> Config {
        Config {
            scan: ScanConfig {
                enabled: true,
                movie_dirs: vec![movie_dir.to_string_lossy().into_owned()],
                interval_hours: 6,
                worker_poll_secs: 5,
                refresh_interval_hours: 1,
                refresh_batch_size: 60,
                ssh_key_path: None,
            },
            tmdb: TmdbConfig {
                api_key: "test-key".into(),
                language: "zh-CN".into(),
                auto_confirm_threshold: 0.85,
                proxy: None,
            },
            llm: LlmConfig {
                backend: Default::default(),
                base_url: "http://localhost".into(),
                api_key: String::new(),
                model: "stub".into(),
            },
            server: ServerConfig {
                host: "127.0.0.1".into(),
                port: 0,
            },
            database: DatabaseConfig {
                path: ":memory:".into(),
            },
            auth: AuthConfig {
                jwt_secret: "test-secret".into(),
                jwt_expiry_days: 30,
            },
            qbittorrent: Default::default(),
        }
    }

    fn make_dirs(root: &Path, names: &[&str]) {
        for n in names {
            fs::create_dir_all(root.join(n)).unwrap();
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn scan_cycle_discovers_and_parses_new_dirs(pool: SqlitePool) {
        let tmp = TempDir::new().unwrap();
        make_dirs(
            tmp.path(),
            &["Inception (2010)", "Parasite (2019)", "Arrival (2016)"],
        );
        let config = test_config(tmp.path());

        run_scan_cycle(&pool, &config).await.unwrap();

        // All 3 dirs should be tracked, flipped to 'parsed', and have a
        // tmdb_search task queued.
        let dir_rows: Vec<(String, String)> =
            sqlx::query_as("SELECT dir_name, scan_status FROM media_dirs ORDER BY dir_name")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(dir_rows.len(), 3);
        for (_, status) in &dir_rows {
            assert_eq!(status, "parsed");
        }

        let pending_search: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM tasks WHERE task_type = 'tmdb_search' AND status = 'pending'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(pending_search, 3);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn scan_cycle_task_payload_has_parsed_fields(pool: SqlitePool) {
        let tmp = TempDir::new().unwrap();
        make_dirs(tmp.path(), &["Inception.2010.1080p.BluRay"]);
        let config = test_config(tmp.path());

        run_scan_cycle(&pool, &config).await.unwrap();

        let payload: String =
            sqlx::query_scalar("SELECT payload FROM tasks WHERE task_type = 'tmdb_search' LIMIT 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(v["title"].as_str().unwrap(), "Inception");
        assert_eq!(v["year"].as_u64(), Some(2010));
        assert!(v["dir_id"].as_i64().unwrap() > 0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn scan_cycle_is_idempotent_on_unchanged_dirs(pool: SqlitePool) {
        let tmp = TempDir::new().unwrap();
        make_dirs(tmp.path(), &["Movie A (2020)"]);
        let config = test_config(tmp.path());

        run_scan_cycle(&pool, &config).await.unwrap();
        run_scan_cycle(&pool, &config).await.unwrap();

        let dir_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media_dirs")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(dir_count, 1, "unchanged dir should not be re-inserted");

        // Only the first cycle created a task. The second cycle found the dir
        // already registered (not 'new'), so no second task was queued.
        let task_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM tasks WHERE task_type = 'tmdb_search'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(task_count, 1);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn scan_cycle_marks_removed_dirs_as_deleted(pool: SqlitePool) {
        let tmp = TempDir::new().unwrap();
        make_dirs(tmp.path(), &["Keeper", "ToDelete"]);
        let config = test_config(tmp.path());

        run_scan_cycle(&pool, &config).await.unwrap();
        assert_eq!(queries::get_all_dir_paths(&pool).await.unwrap().len(), 2);

        fs::remove_dir_all(tmp.path().join("ToDelete")).unwrap();
        run_scan_cycle(&pool, &config).await.unwrap();

        // get_all_dir_paths filters 'deleted', so only Keeper remains.
        let remaining = queries::get_all_dir_paths(&pool).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert!(remaining[0].ends_with("Keeper"));

        // The deleted row still exists in the table, just flipped to 'deleted'.
        let deleted_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM media_dirs WHERE scan_status = 'deleted'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(deleted_count, 1);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn scan_cycle_missing_movie_dir_skips_gracefully(pool: SqlitePool) {
        let config = test_config(Path::new("/nonexistent/path/for/test"));
        // Multi-dir scan skips non-existent directories instead of failing
        let result = run_scan_cycle(&pool, &config).await;
        assert!(result.is_ok());
    }

    // --- process_tmdb_search_tasks ---
    //
    // These tests use wiremock to stand in for api.themoviedb.org and drive
    // the search → decide → write-mapping state machine end-to-end.
    //
    // The scheduler calls search_movie / search_movie_with_lang up to three
    // times per task (primary language, alt title, en-US). We respond to any
    // matching URL with a single canned payload so scoring uses predictable
    // data.

    use wiremock::matchers::{method, path, query_param, query_param_is_missing};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn seed_dir_with_search_task(
        pool: &SqlitePool,
        dir_name: &str,
        title: &str,
        year: Option<u16>,
    ) -> i64 {
        let dir_id = queries::insert_media_dir(pool, &format!("/fake/{}", dir_name), dir_name)
            .await
            .unwrap();
        let payload = serde_json::json!({
            "dir_id": dir_id,
            "title": title,
            "alt_title": null,
            "year": year,
        });
        queries::insert_task(pool, "tmdb_search", &payload.to_string())
            .await
            .unwrap();
        dir_id
    }

    fn search_response(id: i64, title: &str, release_date: &str) -> serde_json::Value {
        serde_json::json!({
            "results": [{
                "id": id,
                "title": title,
                "original_title": title,
                "release_date": release_date,
                "overview": "synopsis",
                "poster_path": "/p.jpg",
                "vote_average": 8.5,
                "vote_count": 1000,
                "popularity": 80.0,
                "genre_ids": [18],
                "original_language": "en"
            }],
            "total_results": 1
        })
    }

    async fn mock_tmdb_server_with_hit(
        id: i64,
        title: &str,
        release_date: &str,
    ) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/search/movie"))
            .respond_with(ResponseTemplate::new(200).set_body_json(search_response(
                id,
                title,
                release_date,
            )))
            .mount(&server)
            .await;
        server
    }

    async fn mock_tmdb_server_empty() -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/search/movie"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "results": [],
                "total_results": 0
            })))
            .mount(&server)
            .await;
        server
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn tmdb_search_auto_confirms_high_score_match(pool: SqlitePool) {
        let dir_id =
            seed_dir_with_search_task(&pool, "Inception (2010)", "Inception", Some(2010)).await;
        let server = mock_tmdb_server_with_hit(27205, "Inception", "2010-07-16").await;

        let mut config = test_config(Path::new("/tmp"));
        config.tmdb.auto_confirm_threshold = 0.85;
        let client = TmdbClient::with_base_url("k", "zh-CN", &server.uri());

        process_tmdb_search_tasks(&pool, &client, &config).await;

        // Mapping should be 'auto' with a real movie row and a follow-up
        // tmdb_fetch task queued.
        let mapping = queries::get_mapping_by_dir_id(&pool, dir_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mapping.match_status, "auto");
        assert!(mapping.movie_id.is_some());

        let scan_status: String =
            sqlx::query_scalar("SELECT scan_status FROM media_dirs WHERE id = ?")
                .bind(dir_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(scan_status, "matched");

        let fetch_pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM tasks WHERE task_type = 'tmdb_fetch' AND status = 'pending'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(fetch_pending, 1);

        // The original search task is done.
        let done: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM tasks WHERE task_type = 'tmdb_search' AND status = 'done'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(done, 1);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn tmdb_search_pending_when_confidence_middling(pool: SqlitePool) {
        // Parsed title differs enough that Levenshtein drops title score into
        // the 0.5~0.85 band. Year also off by a couple to avoid year bonus.
        let dir_id = seed_dir_with_search_task(
            &pool,
            "Some Weird Flick 2005",
            "Some Weird Flick",
            Some(2005),
        )
        .await;
        let server = mock_tmdb_server_with_hit(111, "Another Movie", "2010-01-01").await;

        let mut config = test_config(Path::new("/tmp"));
        config.tmdb.auto_confirm_threshold = 0.85;
        let client = TmdbClient::with_base_url("k", "zh-CN", &server.uri());

        process_tmdb_search_tasks(&pool, &client, &config).await;

        let mapping = queries::get_mapping_by_dir_id(&pool, dir_id)
            .await
            .unwrap()
            .unwrap();
        // Could be pending or failed depending on score. We only care that it
        // is NOT auto and does NOT have a movie bound.
        assert_ne!(mapping.match_status, "auto");
        assert!(mapping.movie_id.is_none());

        let scan_status: String =
            sqlx::query_scalar("SELECT scan_status FROM media_dirs WHERE id = ?")
                .bind(dir_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        // Either 'parsed' (pending decision) or 'failed' — never 'matched'.
        assert_ne!(scan_status, "matched");

        // No tmdb_fetch follow-up should be queued for non-auto decisions.
        let fetch_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM tasks WHERE task_type = 'tmdb_fetch'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(fetch_count, 0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn tmdb_search_failed_when_no_results(pool: SqlitePool) {
        let dir_id =
            seed_dir_with_search_task(&pool, "Obscure Thing", "Obscure Thing", Some(1999)).await;
        let server = mock_tmdb_server_empty().await;

        let config = test_config(Path::new("/tmp"));
        let client = TmdbClient::with_base_url("k", "zh-CN", &server.uri());

        process_tmdb_search_tasks(&pool, &client, &config).await;

        let mapping = queries::get_mapping_by_dir_id(&pool, dir_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mapping.match_status, "failed");
        assert!(mapping.movie_id.is_none());

        let scan_status: String =
            sqlx::query_scalar("SELECT scan_status FROM media_dirs WHERE id = ?")
                .bind(dir_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(scan_status, "failed");

        let done: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM tasks WHERE task_type = 'tmdb_search' AND status = 'done'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(done, 1);
    }

    /// Mock that returns empty for year-filtered requests but a real hit for
    /// year-less requests. Mirrors the production case where TMDB has the film
    /// but its `primary_release_date` lives in a different year than the dir
    /// states (festival vs theatrical vs streaming-release wobble).
    async fn mock_tmdb_year_strict_empty_yearless_hits(
        id: i64,
        title: &str,
        release_date: &str,
        strict_year: u16,
    ) -> MockServer {
        let server = MockServer::start().await;
        // Year-strict requests draw blanks
        Mock::given(method("GET"))
            .and(path("/search/movie"))
            .and(query_param("year", strict_year.to_string().as_str()))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "results": [],
                "total_results": 0
            })))
            .mount(&server)
            .await;
        // Year-less retry hits — release_date is in a different year so the
        // matcher can score year off-by-some
        Mock::given(method("GET"))
            .and(path("/search/movie"))
            .and(query_param_is_missing("year"))
            .respond_with(ResponseTemplate::new(200).set_body_json(search_response(
                id,
                title,
                release_date,
            )))
            .mount(&server)
            .await;
        server
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn tmdb_search_falls_back_to_yearless_when_year_strict_empty(pool: SqlitePool) {
        // Dir says 1999. TMDB returns the film only when we drop the `year=`
        // filter (release_date ends up as 2000 — same shape as Beau Travail
        // 1999 Venice / 2000 France theatrical). Without the fallback this
        // would land at confidence 0.0 / status='failed'. With it, the matcher
        // scores title-perfect + off-by-one and auto-confirms.
        let dir_id = seed_dir_with_search_task(&pool, "Inception 1999", "Inception", Some(1999)).await;
        let server = mock_tmdb_year_strict_empty_yearless_hits(27205, "Inception", "2000-07-16", 1999).await;

        let mut config = test_config(Path::new("/tmp"));
        config.tmdb.auto_confirm_threshold = 0.85;
        let client = TmdbClient::with_base_url("k", "zh-CN", &server.uri());

        process_tmdb_search_tasks(&pool, &client, &config).await;

        let mapping = queries::get_mapping_by_dir_id(&pool, dir_id)
            .await
            .unwrap()
            .unwrap();
        // Year-less retry brought the candidate; matcher scored
        // title 0.6 + off-by-one 0.25 + popularity 0.08 ≈ 0.93 → auto.
        assert_eq!(
            mapping.match_status, "auto",
            "year-less fallback should bring the candidate and auto-confirm; confidence={:?}",
            mapping.confidence,
        );
        assert!(mapping.movie_id.is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn tmdb_search_yearless_fallback_skipped_when_no_year(pool: SqlitePool) {
        // No year on the dir → no year filter to drop. Yearless mock returns
        // a result, year-strict mock would fire on year requests but we never
        // send any. Verifies the guard `year_u32.is_some()` actually gates
        // the fallback (we don't double-search every dir).
        let dir_id = seed_dir_with_search_task(&pool, "Inception", "Inception", None).await;
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/search/movie"))
            .and(query_param_is_missing("year"))
            .respond_with(ResponseTemplate::new(200).set_body_json(search_response(
                27205,
                "Inception",
                "2010-07-16",
            )))
            // expect:
            // - 2 calls (zh-CN primary, en-US) for the no-year-from-the-start case
            // - NOT 4 (which would mean we double-searched)
            .expect(2)
            .mount(&server)
            .await;

        let config = test_config(Path::new("/tmp"));
        let client = TmdbClient::with_base_url("k", "zh-CN", &server.uri());
        process_tmdb_search_tasks(&pool, &client, &config).await;

        let mapping = queries::get_mapping_by_dir_id(&pool, dir_id)
            .await
            .unwrap()
            .unwrap();
        // Title perfect + no year + popularity 0.08 = 0.68 → pending (sanity).
        assert!(mapping.match_status == "pending" || mapping.match_status == "auto");
        // Server.expect(2) is verified on drop; fewer or more = panic.
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn tmdb_search_uses_sidecar_evidence_to_rescue_garbage_dir_name(pool: SqlitePool) {
        // Real-world case: parent dir is a Blu-ray disc volume label like
        // "BRIDGE_O_T_RIVER_KWAI_UHD_EUR_BLUEBIRD" (parser produces a noisy
        // title) but a sub-directory inside has the proper "Title.Year..."
        // naming, AND BDMV/META/DL/bdmt_eng.xml has the disc title. The
        // sidecar collector pulls both into the candidate set, the year-bearing
        // subdir lifts the score above auto threshold.
        use wiremock::matchers::query_param;

        let tmp = TempDir::new().unwrap();
        let dir_path = tmp.path().join("BRIDGE_O_T_RIVER_KWAI_UHD_EUR_BLUEBIRD");
        std::fs::create_dir_all(&dir_path).unwrap();

        // Inner subdir with proper naming (the savior signal)
        std::fs::create_dir_all(
            dir_path.join("The.Bridge.on.the.River.Kwai.1957.2160p.UHD.Blu-ray-SharpHD"),
        )
        .unwrap();

        // BDMV with META XML — also contributes a candidate (title only)
        let meta_dir = dir_path.join("BDMV").join("META").join("DL");
        std::fs::create_dir_all(&meta_dir).unwrap();
        let xml = r#"<?xml version="1.0"?>
<disclib><discinfo><title><name>The Bridge on the River Kwai</name></title></discinfo></disclib>"#;
        std::fs::write(meta_dir.join("bdmt_eng.xml"), xml).unwrap();

        let dir_id = queries::insert_media_dir(
            &pool,
            dir_path.to_str().unwrap(),
            "BRIDGE_O_T_RIVER_KWAI_UHD_EUR_BLUEBIRD",
        )
        .await
        .unwrap();
        let parsed = parser::parse_directory_name("BRIDGE_O_T_RIVER_KWAI_UHD_EUR_BLUEBIRD");
        let payload = serde_json::json!({
            "dir_id": dir_id,
            "title": parsed.title,
            "alt_title": parsed.alt_title,
            "year": parsed.year,
        });
        queries::insert_task(&pool, "tmdb_search", &payload.to_string())
            .await
            .unwrap();

        // Mock TMDB: only "The Bridge on the River Kwai" returns a hit. Other
        // queries (incl. parent's noisy parser output) → empty.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/search/movie"))
            .and(query_param("query", "The Bridge on the River Kwai"))
            .respond_with(ResponseTemplate::new(200).set_body_json(search_response(
                289,
                "The Bridge on the River Kwai",
                "1957-12-14",
            )))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/search/movie"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "results": [],
                "total_results": 0
            })))
            .mount(&server)
            .await;

        let mut config = test_config(Path::new("/tmp"));
        config.tmdb.auto_confirm_threshold = 0.85;
        let client = TmdbClient::with_base_url("k", "zh-CN", &server.uri());

        process_tmdb_search_tasks(&pool, &client, &config).await;

        let mapping = queries::get_mapping_by_dir_id(&pool, dir_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            mapping.match_status, "auto",
            "sidecar (inner subdir + BDMV META) should rescue this dir"
        );
        assert!(mapping.movie_id.is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn tmdb_search_bdmv_meta_alone_yields_pending_with_candidates(pool: SqlitePool) {
        // GRAVITY_HDCLUB-like case: BDMV META gives "Gravity" but no year. We
        // shouldn't auto-confirm without year corroboration, but mapping
        // should be 'pending' with candidates populated (vs 'failed' before).
        use wiremock::matchers::query_param;

        let tmp = TempDir::new().unwrap();
        let dir_path = tmp.path().join("GRAVITY_HDCLUB");
        std::fs::create_dir_all(&dir_path).unwrap();
        let meta_dir = dir_path.join("BDMV").join("META").join("DL");
        std::fs::create_dir_all(&meta_dir).unwrap();
        std::fs::write(
            meta_dir.join("bdmt_eng.xml"),
            r#"<disclib><discinfo><title><name>Gravity</name></title></discinfo></disclib>"#,
        )
        .unwrap();

        let dir_id =
            queries::insert_media_dir(&pool, dir_path.to_str().unwrap(), "GRAVITY_HDCLUB")
                .await
                .unwrap();
        let parsed = parser::parse_directory_name("GRAVITY_HDCLUB");
        let payload = serde_json::json!({
            "dir_id": dir_id,
            "title": parsed.title,
            "alt_title": parsed.alt_title,
            "year": parsed.year,
        });
        queries::insert_task(&pool, "tmdb_search", &payload.to_string())
            .await
            .unwrap();

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/search/movie"))
            .and(query_param("query", "Gravity"))
            .respond_with(ResponseTemplate::new(200).set_body_json(search_response(
                49047,
                "Gravity",
                "2013-10-04",
            )))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/search/movie"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "results": [], "total_results": 0
            })))
            .mount(&server)
            .await;

        let mut config = test_config(Path::new("/tmp"));
        config.tmdb.auto_confirm_threshold = 0.85;
        let client = TmdbClient::with_base_url("k", "zh-CN", &server.uri());

        process_tmdb_search_tasks(&pool, &client, &config).await;

        let mapping = queries::get_mapping_by_dir_id(&pool, dir_id)
            .await
            .unwrap()
            .unwrap();
        // Was 'failed' before sidecar; now should be 'pending' with the
        // Gravity candidate available for human binding.
        assert_eq!(mapping.match_status, "pending");
        let candidates_json = mapping.candidates.unwrap_or_default();
        assert!(
            candidates_json.contains("Gravity"),
            "candidates should include Gravity from BDMV META; got {}",
            candidates_json
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn tmdb_search_downgrades_auto_to_pending_on_ambiguous_top(pool: SqlitePool) {
        // Two distinct films both score above auto threshold (sidecar pulled
        // in a wrong-but-tempting candidate). Conflict guard should downgrade
        // the decision to pending instead of coin-flipping a binding.
        use wiremock::matchers::query_param;

        let tmp = TempDir::new().unwrap();
        // Parent dir parses to "Foo" with year 2010. Inner subdir hints at
        // "Bar 2010". Both will return same-year exact-title TMDB hits → both
        // get score ≈ 0.6 (title 0.6 + year 0.3) = 0.98 — auto threshold.
        let dir_path = tmp.path().join("Foo 2010");
        std::fs::create_dir_all(&dir_path).unwrap();
        std::fs::create_dir_all(dir_path.join("Bar 2010")).unwrap();

        let dir_id =
            queries::insert_media_dir(&pool, dir_path.to_str().unwrap(), "Foo 2010")
                .await
                .unwrap();
        let parsed = parser::parse_directory_name("Foo 2010");
        let payload = serde_json::json!({
            "dir_id": dir_id,
            "title": parsed.title,
            "alt_title": parsed.alt_title,
            "year": parsed.year,
        });
        queries::insert_task(&pool, "tmdb_search", &payload.to_string())
            .await
            .unwrap();

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/search/movie"))
            .and(query_param("query", "Foo"))
            .respond_with(ResponseTemplate::new(200).set_body_json(search_response(
                100, "Foo", "2010-01-01",
            )))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/search/movie"))
            .and(query_param("query", "Bar"))
            .respond_with(ResponseTemplate::new(200).set_body_json(search_response(
                200, "Bar", "2010-01-01",
            )))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/search/movie"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "results": [], "total_results": 0
            })))
            .mount(&server)
            .await;

        let mut config = test_config(Path::new("/tmp"));
        config.tmdb.auto_confirm_threshold = 0.85;
        let client = TmdbClient::with_base_url("k", "zh-CN", &server.uri());

        process_tmdb_search_tasks(&pool, &client, &config).await;

        let mapping = queries::get_mapping_by_dir_id(&pool, dir_id)
            .await
            .unwrap()
            .unwrap();
        // Both Foo and Bar score equally high; conflict guard downgrades to
        // pending (NOT auto) so a human can pick.
        assert_eq!(
            mapping.match_status, "pending",
            "ambiguous top candidates must NOT auto-confirm"
        );
        assert!(mapping.movie_id.is_none());
        // Both candidates should be in the snapshot for the user to choose.
        let cand_json = mapping.candidates.unwrap_or_default();
        assert!(cand_json.contains("Foo") && cand_json.contains("Bar"));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn tmdb_search_with_no_pending_task_is_noop(pool: SqlitePool) {
        let server = mock_tmdb_server_empty().await;
        let config = test_config(Path::new("/tmp"));
        let client = TmdbClient::with_base_url("k", "zh-CN", &server.uri());

        // No task has been queued — function should return without touching
        // the DB and without calling the server.
        process_tmdb_search_tasks(&pool, &client, &config).await;

        let mapping_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM dir_movie_mappings")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(mapping_count, 0);
    }

    async fn seed_movie_for_refresh(
        pool: &SqlitePool,
        tmdb_id: i64,
        source: &str,
        version: Option<i32>,
    ) -> i64 {
        let movie_id = queries::insert_movie(
            pool, tmdb_id, "t", None, None, None, None, "[]", None, None, None, None, "[]",
            None, None, "[]", None, None, None, source,
        )
        .await
        .unwrap();
        if let Some(v) = version {
            sqlx::query("UPDATE movies SET tmdb_fetch_version = ? WHERE id = ?")
                .bind(v)
                .bind(movie_id)
                .execute(pool)
                .await
                .unwrap();
        }
        movie_id
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn refresh_stale_movies_enqueues_fetch_tasks_respecting_version_and_source(
        pool: SqlitePool,
    ) {
        // 三部电影：
        //  - id=100 library, version=NULL     → 应入队，fetch_related=true
        //  - id=200 related, version=NULL     → 应入队，fetch_related=false
        //  - id=300 library, version=CURRENT  → 跳过
        seed_movie_for_refresh(&pool, 100, "library", None).await;
        seed_movie_for_refresh(&pool, 200, "related", None).await;
        seed_movie_for_refresh(&pool, 300, "library", Some(CURRENT_TMDB_FETCH_VERSION)).await;

        refresh_stale_movies(&pool, 60).await;

        let payloads: Vec<String> = sqlx::query_scalar(
            "SELECT payload FROM tasks WHERE task_type='tmdb_fetch' ORDER BY id",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(payloads.len(), 2, "only the two stale rows should be queued");

        let joined = payloads.join("\n");
        assert!(joined.contains("\"tmdb_id\":100") && joined.contains("\"fetch_related\":true"));
        assert!(joined.contains("\"tmdb_id\":200") && joined.contains("\"fetch_related\":false"));
        assert!(!joined.contains("\"tmdb_id\":300"), "up-to-date row must not be queued");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn refresh_stale_movies_skips_rows_with_existing_pending_task(pool: SqlitePool) {
        seed_movie_for_refresh(&pool, 100, "library", None).await;

        // 先手工塞一个 pending 的 tmdb_fetch 任务占位。
        queries::insert_task(
            &pool,
            "tmdb_fetch",
            &serde_json::json!({ "tmdb_id": 100, "fetch_related": true }).to_string(),
        )
        .await
        .unwrap();

        refresh_stale_movies(&pool, 60).await;

        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM tasks WHERE task_type='tmdb_fetch'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 1, "must not enqueue a duplicate task for the same tmdb_id");
    }

    // --- pick_best_poster_path ---

    #[derive(Debug)]
    struct P { lang: Option<&'static str>, vote: f64, path: &'static str }
    impl PosterEntry for P {
        fn iso_639_1(&self) -> Option<&str> { self.lang }
        fn vote_average(&self) -> f64 { self.vote }
        fn file_path(&self) -> &str { self.path }
    }

    #[test]
    fn poster_picker_prefers_original_language_over_higher_rated_others() {
        // Reproducing the Léon (id=618) bug: orig_lang=fr, no fr posters, but
        // zh has the highest score. Without the en fallback we'd return zh —
        // that's the surprise. With it, en wins.
        let posters = vec![
            P { lang: Some("zh"), vote: 8.0, path: "/zh.jpg" },
            P { lang: Some("en"), vote: 5.7, path: "/en.jpg" },
            P { lang: None, vote: 2.5, path: "/notext.jpg" },
        ];
        assert_eq!(pick_best_poster_path("fr", &posters), Some("/en.jpg"));
    }

    #[test]
    fn poster_picker_returns_original_language_when_present() {
        let posters = vec![
            P { lang: Some("fr"), vote: 4.0, path: "/fr.jpg" },
            P { lang: Some("en"), vote: 9.0, path: "/en.jpg" },
        ];
        assert_eq!(pick_best_poster_path("fr", &posters), Some("/fr.jpg"));
    }

    #[test]
    fn poster_picker_picks_highest_rated_within_original_language() {
        let posters = vec![
            P { lang: Some("fr"), vote: 4.0, path: "/fr-low.jpg" },
            P { lang: Some("fr"), vote: 8.0, path: "/fr-hi.jpg" },
            P { lang: Some("en"), vote: 9.0, path: "/en.jpg" },
        ];
        assert_eq!(pick_best_poster_path("fr", &posters), Some("/fr-hi.jpg"));
    }

    #[test]
    fn poster_picker_falls_through_to_any_when_no_orig_no_en() {
        let posters = vec![
            P { lang: Some("zh"), vote: 6.0, path: "/zh.jpg" },
            P { lang: Some("ja"), vote: 8.0, path: "/ja.jpg" },
        ];
        assert_eq!(pick_best_poster_path("fr", &posters), Some("/ja.jpg"));
    }

    #[test]
    fn poster_picker_handles_orig_lang_eq_en() {
        // When orig_lang is en itself, the en-fallback step is a no-op (we
        // don't double-search the same lang). Ensure we still fall through.
        let posters = vec![
            P { lang: Some("zh"), vote: 8.0, path: "/zh.jpg" },
        ];
        assert_eq!(pick_best_poster_path("en", &posters), Some("/zh.jpg"));
    }

    #[test]
    fn poster_picker_returns_none_for_empty() {
        let posters: Vec<P> = vec![];
        assert_eq!(pick_best_poster_path("en", &posters), None);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn refresh_stale_movies_respects_batch_size(pool: SqlitePool) {
        for i in 0..5 {
            seed_movie_for_refresh(&pool, 1000 + i, "library", None).await;
        }

        refresh_stale_movies(&pool, 2).await;

        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM tasks WHERE task_type='tmdb_fetch'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 2);
    }
}
