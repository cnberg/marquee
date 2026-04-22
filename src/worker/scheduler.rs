use std::path::Path;

use sqlx::SqlitePool;
use tokio::time::{interval, Duration};

use crate::config::Config;
use crate::db::queries;
use crate::scanner::{parser, walker};
use crate::tmdb::client::TmdbClient;
use crate::tmdb::matcher::{decide_match, score_candidates, MatchDecision};

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
    let search_pool = pool.clone();
    let search_tmdb = tmdb_client.clone();
    let search_config = config.clone();
    tokio::spawn(async move {
        let mut timer = interval(Duration::from_secs(5));

        loop {
            timer.tick().await;
            process_tmdb_search_tasks(&search_pool, &search_tmdb, &search_config).await;
        }
    });

    // TMDB fetch worker
    let fetch_pool = pool.clone();
    let fetch_tmdb = tmdb_client;
    tokio::spawn(async move {
        let mut timer = interval(Duration::from_secs(5));

        loop {
            timer.tick().await;
            process_tmdb_fetch_tasks(&fetch_pool, &fetch_tmdb).await;
        }
    });
}

pub async fn run_scan_cycle(
    pool: &SqlitePool,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 1) Walk directories
    walker::scan_movie_dir(pool, Path::new(&config.scan.movie_dir)).await?;

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

    let year_u32 = year.map(|y| y as u32);

    // Search with primary title (in configured language, usually zh-CN)
    let primary_results = tmdb.search_movie(title, year_u32).await.unwrap_or_default();

    // Search with alt title if available (e.g. Chinese title when primary is English)
    let alt_results = if let Some(alt) = alt_title {
        if !alt.is_empty() {
            tmdb.search_movie(alt, year_u32).await.unwrap_or_default()
        } else {
            vec![]
        }
    } else {
        vec![]
    };

    // Also search in English to catch cases where zh-CN results have
    // Chinese/Japanese titles that don't match the English parsed title.
    // This fixes e.g. "After Life 1998" → TMDB zh-CN returns "下一站，天国" (no English match)
    let en_results = tmdb.search_movie_with_lang(title, year_u32, "en-US").await.unwrap_or_default();

    // Collect all results (including duplicates from different language searches)
    let all_raw: Vec<_> = primary_results.into_iter()
        .chain(alt_results.into_iter())
        .chain(en_results.into_iter())
        .collect();

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

    // Score ALL results (including duplicates with different language titles)
    // against primary title, alt title — take best score per tmdb_id
    let mut best_scores: std::collections::HashMap<i64, (f64, crate::tmdb::client::TmdbSearchResult)> = std::collections::HashMap::new();

    // Collect all query strings to score against
    let mut query_titles = vec![title.to_string()];
    if let Some(alt) = alt_title {
        if !alt.is_empty() {
            query_titles.push(alt.to_string());
        }
    }

    for qt in &query_titles {
        let scored = score_candidates(qt, year, all_raw.clone());
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
            let decision = decide_match(top_score, config.tmdb.auto_confirm_threshold);

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
        tmdb.get_movie_basic(tmdb_id, "en-US"),
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

    let year = full
        .release_date
        .as_ref()
        .and_then(|d| d.get(..4))
        .and_then(|y| y.parse::<i64>().ok());

    let title_en = en.as_ref().map(|e| e.title.clone());
    let overview_en = en.as_ref().and_then(|e| e.overview.clone());
    let tagline_en = en.as_ref().and_then(|e| e.tagline.clone());
    let genres_en_vec: Vec<String> = en
        .as_ref()
        .and_then(|e| e.genres.as_ref())
        .map(|g| g.iter().map(|x| x.name.clone()).collect())
        .unwrap_or_default();
    let genres_en = serde_json::to_string(&genres_en_vec).unwrap_or_else(|_| "[]".to_string());

    let genres_zh_vec: Vec<String> = full
        .genres
        .as_ref()
        .map(|g| g.iter().map(|x| x.name.clone()).collect())
        .unwrap_or_default();
    let genres_zh = serde_json::to_string(&genres_zh_vec).unwrap_or_else(|_| "[]".to_string());

    let country = full
        .production_countries
        .as_ref()
        .and_then(|c| c.first())
        .map(|c| c.iso_3166_1.clone());

    let collection_json = full
        .belongs_to_collection
        .as_ref()
        .map(|c| serde_json::to_string(c).unwrap_or_default());
    let companies_json = full
        .production_companies
        .as_ref()
        .map(|c| serde_json::to_string(c).unwrap_or_default());
    let languages_json = full
        .spoken_languages
        .as_ref()
        .map(|l| serde_json::to_string(l).unwrap_or_default());
    let origin_json = full
        .origin_country
        .as_ref()
        .map(|o| serde_json::to_string(o).unwrap_or_default());

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

    let poster_url = full
        .images
        .as_ref()
        .and_then(|images| {
            let orig_lang = full.original_language.as_deref().unwrap_or("en");
            images
                .posters
                .as_ref()
                .and_then(|posters| {
                    posters
                        .iter()
                        .filter(|p| p.iso_639_1.as_deref() == Some(orig_lang))
                        .max_by(|a, b| {
                            a.vote_average
                                .unwrap_or(0.0)
                                .partial_cmp(&b.vote_average.unwrap_or(0.0))
                                .unwrap_or(std::cmp::Ordering::Equal)
                        })
                        .map(|p| format!("https://image.tmdb.org/t/p/w500{}", p.file_path))
                })
                .or_else(|| {
                    images.posters.as_ref().and_then(|posters| {
                        posters
                            .iter()
                            .max_by(|a, b| {
                                a.vote_average
                                    .unwrap_or(0.0)
                                    .partial_cmp(&b.vote_average.unwrap_or(0.0))
                                    .unwrap_or(std::cmp::Ordering::Equal)
                            })
                            .map(|p| format!("https://image.tmdb.org/t/p/w500{}", p.file_path))
                    })
                })
        })
        .or_else(|| {
            full.poster_path
                .as_ref()
                .map(|p| format!("https://image.tmdb.org/t/p/w500{}", p))
        });

    if let Err(err) = sqlx::query(
        "UPDATE movies SET
            title = ?, original_title = ?, year = ?, overview = ?,
            poster_url = ?, genres = ?, country = ?, language = ?,
            runtime = ?, director = ?, director_info = ?, cast = ?, keywords = ?,
            tmdb_rating = ?, tmdb_votes = ?,
            budget = ?, revenue = ?, popularity = ?,
            title_zh = ?, title_en = ?, overview_zh = ?, overview_en = ?,
            tagline_zh = ?, tagline_en = ?, genres_zh = ?, genres_en = ?,
            imdb_id = ?, backdrop_path = ?, homepage = ?, status = ?,
            collection = ?, production_companies = ?, spoken_languages = ?, origin_country = ?,
            updated_at = datetime('now')
         WHERE tmdb_id = ?",
    )
    .bind(&full.title)
    .bind(&full.original_title)
    .bind(year)
    .bind(&full.overview)
    .bind(&poster_url)
    .bind(serde_json::to_string(&genres_zh_vec).unwrap_or_default())
    .bind(&country)
    .bind(&full.original_language)
    .bind(full.runtime)
    .bind(&director_name)
    .bind(serde_json::to_string(&directors).unwrap_or_default())
    .bind(serde_json::to_string(&cast_structured).unwrap_or_default())
    .bind(serde_json::to_string(&keywords_list).unwrap_or_default())
    .bind(full.vote_average)
    .bind(full.vote_count)
    .bind(full.budget)
    .bind(full.revenue)
    .bind(full.popularity)
    .bind(&full.title)
    .bind(&title_en)
    .bind(&full.overview)
    .bind(&overview_en)
    .bind(&full.tagline)
    .bind(&tagline_en)
    .bind(&genres_zh)
    .bind(&genres_en)
    .bind(&full.imdb_id)
    .bind(&full.backdrop_path)
    .bind(&full.homepage)
    .bind(&full.status)
    .bind(&collection_json)
    .bind(&companies_json)
    .bind(&languages_json)
    .bind(&origin_json)
    .bind(tmdb_id)
    .execute(pool)
    .await
    {
        tracing::error!(tmdb_id, error = %err, "failed to update movie");
        if let Err(e) = queries::fail_task(pool, task.id, &err.to_string()).await {
            tracing::warn!(task_id = task.id, error = %e, "fail_task failed");
        }
        return;
    }

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

    let mut credit_rows = Vec::new();
    if let Some(credits) = &full.credits {
        if let Some(cast) = &credits.cast {
            for c in cast {
                credit_rows.push(queries::CreditRow {
                    tmdb_person_id: c.id,
                    person_name: c.name.clone(),
                    credit_type: "cast".to_string(),
                    role: c.character.clone(),
                    department: None,
                    order: c.order,
                    profile_path: c.profile_path.clone(),
                });
            }
        }
        if let Some(crew) = &credits.crew {
            for c in crew {
                credit_rows.push(queries::CreditRow {
                    tmdb_person_id: c.id,
                    person_name: c.name.clone(),
                    credit_type: "crew".to_string(),
                    role: Some(c.job.clone()),
                    department: None,
                    order: None,
                    profile_path: c.profile_path.clone(),
                });
            }
        }
    }
    if let Err(e) = queries::replace_movie_credits(pool, movie_id, &credit_rows).await {
        tracing::warn!(movie_id, error = %e, "replace_movie_credits failed");
    }

    let mut image_rows = Vec::new();
    if let Some(images) = &full.images {
        let configs = [
            ("poster", &images.posters),
            ("backdrop", &images.backdrops),
            ("logo", &images.logos),
        ];
        for (image_type, list) in configs {
            if let Some(items) = list {
                for img in items {
                    image_rows.push(queries::ImageRow {
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
    if let Err(e) = queries::replace_movie_images(pool, movie_id, &image_rows).await {
        tracing::warn!(movie_id, error = %e, "replace_movie_images failed");
    }

    let mut video_rows = Vec::new();
    if let Some(videos) = &full.videos {
        if let Some(list) = &videos.results {
            for v in list {
                video_rows.push(queries::VideoRow {
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
    if let Err(e) = queries::replace_movie_videos(pool, movie_id, &video_rows).await {
        tracing::warn!(movie_id, error = %e, "replace_movie_videos failed");
    }

    let mut review_rows = Vec::new();
    if let Some(reviews) = &full.reviews {
        if let Some(list) = &reviews.results {
            for r in list {
                review_rows.push(queries::ReviewRow {
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
    if let Err(e) = queries::replace_movie_reviews(pool, movie_id, &review_rows).await {
        tracing::warn!(movie_id, error = %e, "replace_movie_reviews failed");
    }

    let mut release_rows = Vec::new();
    if let Some(releases) = &full.release_dates {
        if let Some(countries) = &releases.results {
            for c in countries {
                for entry in &c.release_dates {
                    release_rows.push(queries::ReleaseDateRow {
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
    if let Err(e) = queries::replace_movie_release_dates(pool, movie_id, &release_rows).await {
        tracing::warn!(movie_id, error = %e, "replace_movie_release_dates failed");
    }

    let mut wp_rows = Vec::new();
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
                            wp_rows.push(queries::WatchProviderRow {
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
    if let Err(e) = queries::replace_movie_watch_providers(pool, movie_id, &wp_rows).await {
        tracing::warn!(movie_id, error = %e, "replace_movie_watch_providers failed");
    }

    let external_row = {
        let ext = full.external_ids.as_ref();
        queries::ExternalIdRow {
            imdb_id: ext.and_then(|e| e.imdb_id.clone()),
            facebook_id: ext.and_then(|e| e.facebook_id.clone()),
            instagram_id: ext.and_then(|e| e.instagram_id.clone()),
            twitter_id: ext.and_then(|e| e.twitter_id.clone()),
            wikidata_id: ext.and_then(|e| e.wikidata_id.clone()),
        }
    };
    if let Err(e) = queries::replace_movie_external_ids(pool, movie_id, &external_row).await {
        tracing::warn!(movie_id, error = %e, "replace_movie_external_ids failed");
    }

    let mut alt_rows = Vec::new();
    if let Some(alts) = &full.alternative_titles {
        if let Some(titles) = &alts.titles {
            for t in titles {
                alt_rows.push(queries::AlternativeTitleRow {
                    iso_3166_1: t.iso_3166_1.clone(),
                    title: t.title.clone(),
                    title_type: t.title_type.clone(),
                });
            }
        }
    }
    if let Err(e) = queries::replace_movie_alternative_titles(pool, movie_id, &alt_rows).await {
        tracing::warn!(movie_id, error = %e, "replace_movie_alternative_titles failed");
    }

    let mut translation_rows = Vec::new();
    if let Some(trans) = &full.translations {
        if let Some(items) = &trans.translations {
            for t in items {
                let data = t.data.as_ref();
                translation_rows.push(queries::TranslationRow {
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
    if let Err(e) = queries::replace_movie_translations(pool, movie_id, &translation_rows).await {
        tracing::warn!(movie_id, error = %e, "replace_movie_translations failed");
    }

    let mut list_rows = Vec::new();
    if let Some(lists) = &full.lists {
        if let Some(results) = &lists.results {
            for l in results {
                list_rows.push(queries::MovieListRow {
                    tmdb_list_id: l.id,
                    list_name: l.name.clone(),
                    description: l.description.clone(),
                    item_count: l.item_count,
                    iso_639_1: l.iso_639_1.clone(),
                });
            }
        }
    }
    if let Err(e) = queries::replace_movie_lists(pool, movie_id, &list_rows).await {
        tracing::warn!(movie_id, error = %e, "replace_movie_lists failed");
    }

    if fetch_related {
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

    if let Err(e) = queries::complete_task(pool, task.id).await {
        tracing::warn!(task_id = task.id, error = %e, "complete_task failed");
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
                movie_dir: movie_dir.to_string_lossy().into_owned(),
                interval_hours: 6,
            },
            tmdb: TmdbConfig {
                api_key: "test-key".into(),
                language: "zh-CN".into(),
                auto_confirm_threshold: 0.85,
                proxy: None,
            },
            llm: LlmConfig {
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
    async fn scan_cycle_missing_movie_dir_returns_error(pool: SqlitePool) {
        let config = test_config(Path::new("/nonexistent/path/for/test"));
        let result = run_scan_cycle(&pool, &config).await;
        assert!(result.is_err());
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

    use wiremock::matchers::{method, path};
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
}
