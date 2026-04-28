use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{delete, get, post, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::time::Instant;

use crate::api::AppState;
use crate::auth::RequireUser;
use crate::db;

// Benchmark 回归测试：查询题库、历次运行、逐条结果。详见 docs/specs/benchmark.md。
// 这里只有"空壳"代码；用户的 query/expected/结果都落在 runtime DB 里，
// 不会随 publish.sh 流入公仓。

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/benchmark/queries", get(list_queries))
        .route("/benchmark/queries", post(create_query))
        .route("/benchmark/queries/{id}", put(update_query))
        .route("/benchmark/queries/{id}", delete(delete_query))
        .route("/benchmark/queries/{id}/runs", get(get_query_runs))
        .route(
            "/benchmark/queries/{id}/aggregate",
            get(get_query_aggregate),
        )
        .route(
            "/benchmark/queries/{id}/aggregate/movies/{tmdb_id}",
            get(get_query_movie_appearances),
        )
        .route("/benchmark/runs", get(list_runs))
        .route("/benchmark/runs", post(start_run))
        .route("/benchmark/runs/{id}", get(get_run))
        .route("/benchmark/runs/{id}/compare", get(compare_run))
        .route("/benchmark/runs/{id}/baseline", post(set_baseline))
        .route("/benchmark/runs/{id}/cancel", post(cancel_run))
}

// ===== Queries CRUD =====

#[derive(Debug, Deserialize)]
struct UpsertQueryBody {
    query: String,
    note: Option<String>,
    expected_ids: Option<Vec<i64>>,
    #[serde(default)]
    source_history_id: Option<i64>,
    /// 「不应包含」标准答案：picks 中出现任何此 id 即硬否决整条 query。
    /// expected_ids 与 not_expected_ids 不能含相同 tmdb_id。
    #[serde(default)]
    not_expected_ids: Option<Vec<i64>>,
}

fn encode_expected(ids: &Option<Vec<i64>>) -> Option<String> {
    ids.as_ref().and_then(|v| {
        if v.is_empty() {
            None
        } else {
            serde_json::to_string(v).ok()
        }
    })
}

fn decode_expected(raw: Option<&str>) -> Vec<i64> {
    raw.and_then(|s| serde_json::from_str::<Vec<i64>>(s).ok())
        .unwrap_or_default()
}

/// 校验 expected 与 not_expected 不重叠。冲突的 tmdb_id 没有合理解释。
fn validate_expected_disjoint(
    expected: &Option<Vec<i64>>,
    not_expected: &Option<Vec<i64>>,
) -> Result<(), (StatusCode, String)> {
    let exp = expected.as_deref().unwrap_or(&[]);
    let neg = not_expected.as_deref().unwrap_or(&[]);
    if exp.is_empty() || neg.is_empty() {
        return Ok(());
    }
    let exp_set: std::collections::HashSet<i64> = exp.iter().copied().collect();
    let conflict: Vec<i64> = neg.iter().copied().filter(|id| exp_set.contains(id)).collect();
    if !conflict.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "expected_ids and not_expected_ids overlap: {:?}",
                conflict
            ),
        ));
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct QueryView {
    id: i64,
    query: String,
    note: Option<String>,
    expected_ids: Vec<i64>,
    not_expected_ids: Vec<i64>,
    created_at: String,
    updated_at: String,
    source_history_id: Option<i64>,
}

impl From<db::BenchmarkQuery> for QueryView {
    fn from(q: db::BenchmarkQuery) -> Self {
        QueryView {
            id: q.id,
            expected_ids: decode_expected(q.expected_ids.as_deref()),
            not_expected_ids: decode_expected(q.not_expected_ids.as_deref()),
            query: q.query,
            note: q.note,
            created_at: q.created_at,
            updated_at: q.updated_at,
            source_history_id: q.source_history_id,
        }
    }
}

async fn list_queries(
    _user: RequireUser,
    State(state): State<AppState>,
) -> Result<Json<Vec<QueryView>>, (StatusCode, String)> {
    let rows = db::list_benchmark_queries(&state.pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(rows.into_iter().map(QueryView::from).collect()))
}

async fn create_query(
    _user: RequireUser,
    State(state): State<AppState>,
    Json(body): Json<UpsertQueryBody>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if body.query.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "query is empty".to_string()));
    }
    validate_expected_disjoint(&body.expected_ids, &body.not_expected_ids)?;
    let expected = encode_expected(&body.expected_ids);
    let not_expected = encode_expected(&body.not_expected_ids);
    let id = db::insert_benchmark_query(
        &state.pool,
        body.query.trim(),
        body.note.as_deref(),
        expected.as_deref(),
        body.source_history_id,
        not_expected.as_deref(),
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(serde_json::json!({ "id": id })))
}

#[derive(Debug, Serialize)]
struct QueryRunResultView {
    run_id: i64,
    run_started_at: String,
    run_finished_at: Option<String>,
    run_status: String,
    run_note: Option<String>,
    run_is_baseline: bool,
    hit: Option<bool>,
    elapsed_ms: Option<i64>,
    top_movies: serde_json::Value,
    intent: serde_json::Value,
    error: Option<String>,
    coverage_ratio: Option<f64>,
    not_expected_ids: Vec<i64>,
}

async fn get_query_runs(
    _user: RequireUser,
    State(state): State<AppState>,
    Path(query_id): Path<i64>,
) -> Result<Json<Vec<QueryRunResultView>>, (StatusCode, String)> {
    // 不存在的 query_id 返 404，避免与「query 没跑过」的空数组混淆。
    let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM benchmark_queries WHERE id = ?")
        .bind(query_id)
        .fetch_one(&state.pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if exists == 0 {
        return Err((StatusCode::NOT_FOUND, "query not found".to_string()));
    }

    let rows = db::list_query_run_results(&state.pool, query_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let view: Vec<QueryRunResultView> = rows
        .into_iter()
        .map(|r| QueryRunResultView {
            run_id: r.run_id,
            run_started_at: r.run_started_at,
            run_finished_at: r.run_finished_at,
            run_status: r.run_status,
            run_note: r.run_note,
            run_is_baseline: r.run_is_baseline != 0,
            hit: r.hit.map(|v| v != 0),
            elapsed_ms: r.elapsed_ms,
            top_movies: r
                .top_movies_json
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or(serde_json::Value::Array(vec![])),
            intent: r
                .intent_json
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or(serde_json::Value::Null),
            error: r.error,
            coverage_ratio: r.coverage_ratio,
            not_expected_ids: decode_expected(r.not_expected_ids.as_deref()),
        })
        .collect();

    Ok(Json(view))
}

async fn update_query(
    _user: RequireUser,
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(body): Json<UpsertQueryBody>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if body.query.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "query is empty".to_string()));
    }
    validate_expected_disjoint(&body.expected_ids, &body.not_expected_ids)?;
    let expected = encode_expected(&body.expected_ids);
    let not_expected = encode_expected(&body.not_expected_ids);
    let affected = db::update_benchmark_query(
        &state.pool,
        id,
        body.query.trim(),
        body.note.as_deref(),
        expected.as_deref(),
        not_expected.as_deref(),
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if affected == 0 {
        return Err((StatusCode::NOT_FOUND, "query not found".to_string()));
    }
    Ok(Json(serde_json::json!({ "message": "updated" })))
}

async fn delete_query(
    _user: RequireUser,
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let affected = db::delete_benchmark_query(&state.pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if affected == 0 {
        return Err((StatusCode::NOT_FOUND, "query not found".to_string()));
    }
    Ok(Json(serde_json::json!({ "message": "deleted" })))
}

// ===== Runs =====

#[derive(Debug, Deserialize)]
struct StartRunBody {
    note: Option<String>,
}

#[derive(Debug, Serialize)]
struct RunView {
    id: i64,
    started_at: String,
    finished_at: Option<String>,
    status: String,
    total: i64,
    passed: i64,
    failed: i64,
    note: Option<String>,
    is_baseline: bool,
    cancel_requested: bool,
}

impl From<db::BenchmarkRun> for RunView {
    fn from(r: db::BenchmarkRun) -> Self {
        RunView {
            id: r.id,
            started_at: r.started_at,
            finished_at: r.finished_at,
            status: r.status,
            total: r.total,
            passed: r.passed,
            failed: r.failed,
            note: r.note,
            is_baseline: r.is_baseline != 0,
            cancel_requested: r.cancel_requested != 0,
        }
    }
}

#[derive(Debug, Serialize)]
struct ResultView {
    id: i64,
    query_id: i64,
    query_text: String,
    expected_ids: Vec<i64>,
    not_expected_ids: Vec<i64>,
    top_movies: serde_json::Value,
    intent_json: Option<serde_json::Value>,
    hit: Option<bool>,
    coverage_ratio: Option<f64>,
    elapsed_ms: Option<i64>,
    error: Option<String>,
}

fn result_to_view(r: db::BenchmarkResult) -> ResultView {
    let top_movies: serde_json::Value = serde_json::from_str(&r.top_movie_ids)
        .unwrap_or_else(|_| serde_json::Value::Array(vec![]));
    let intent_json: Option<serde_json::Value> = r
        .intent_json
        .as_deref()
        .and_then(|s| serde_json::from_str(s).ok());
    ResultView {
        id: r.id,
        query_id: r.query_id,
        query_text: r.query_snapshot,
        expected_ids: decode_expected(r.expected_ids.as_deref()),
        not_expected_ids: decode_expected(r.not_expected_ids.as_deref()),
        top_movies,
        intent_json,
        hit: r.hit.map(|h| h != 0),
        coverage_ratio: r.coverage_ratio,
        elapsed_ms: r.elapsed_ms,
        error: r.error,
    }
}

#[derive(Debug, Deserialize)]
struct RunListQuery {
    limit: Option<i64>,
}

async fn list_runs(
    _user: RequireUser,
    State(state): State<AppState>,
    Query(params): Query<RunListQuery>,
) -> Result<Json<Vec<RunView>>, (StatusCode, String)> {
    let runs = db::list_benchmark_runs(&state.pool, params.limit.unwrap_or(30))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(runs.into_iter().map(RunView::from).collect()))
}

async fn start_run(
    _user: RequireUser,
    State(state): State<AppState>,
    Json(body): Json<StartRunBody>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if db::get_running_benchmark_run(&state.pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .is_some()
    {
        return Err((StatusCode::CONFLICT, "benchmark already running".to_string()));
    }

    let queries = db::list_benchmark_queries(&state.pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if queries.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "no benchmark queries".to_string()));
    }

    let note = body.note.as_deref().filter(|s| !s.is_empty());
    let run_id =
        db::insert_benchmark_run(&state.pool, queries.len() as i64, note)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let embedding_model = state
        .embedding_model
        .clone()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "embedding model unavailable".to_string()))?;
    let embedding_store = state
        .embedding_store
        .clone()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "embedding store unavailable".to_string()))?;
    let llm = state.llm.clone();
    let pool = state.pool.clone();

    tokio::spawn(async move {
        execute_benchmark_run(pool, llm, embedding_model, embedding_store, run_id, queries).await;
    });

    Ok(Json(serde_json::json!({ "run_id": run_id })))
}

async fn get_run(
    _user: RequireUser,
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let run = db::get_benchmark_run(&state.pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or((StatusCode::NOT_FOUND, "run not found".to_string()))?;
    let queries = db::list_benchmark_queries(&state.pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let queries_map: std::collections::HashMap<i64, db::BenchmarkQuery> =
        queries.into_iter().map(|q| (q.id, q)).collect();
    let results = db::list_benchmark_results(&state.pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let results_view: Vec<ResultView> = results
        .into_iter()
        .map(|mut r| {
            // 若 query 行仍存在，补它现在的 note（query_snapshot 已存本次跑时的文本）
            if let Some(q) = queries_map.get(&r.query_id) {
                if r.expected_ids.is_none() {
                    r.expected_ids = q.expected_ids.clone();
                }
            }
            result_to_view(r)
        })
        .collect();

    Ok(Json(serde_json::json!({
        "run": RunView::from(run),
        "results": results_view,
    })))
}

#[derive(Debug, Deserialize)]
struct CompareQuery {
    baseline: Option<i64>,
}

#[derive(Debug, Serialize)]
struct CompareItem {
    query_id: i64,
    query_text: String,
    expected_ids: Vec<i64>,
    baseline: Option<ResultView>,
    current: Option<ResultView>,
    added_movies: Vec<serde_json::Value>,
    removed_movies: Vec<serde_json::Value>,
    intent_changed: bool,
    hit_delta: Option<i64>,
}

async fn compare_run(
    _user: RequireUser,
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Query(params): Query<CompareQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let current = db::get_benchmark_run(&state.pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or((StatusCode::NOT_FOUND, "run not found".to_string()))?;
    let baseline_id = match params.baseline {
        Some(b) => b,
        None => db::get_baseline_benchmark_run(&state.pool)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .ok_or((StatusCode::BAD_REQUEST, "no baseline configured".to_string()))?
            .id,
    };
    let baseline = db::get_benchmark_run(&state.pool, baseline_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or((StatusCode::NOT_FOUND, "baseline run not found".to_string()))?;

    let current_results = db::list_benchmark_results(&state.pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let baseline_results = db::list_benchmark_results(&state.pool, baseline_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let items = compute_compare(baseline_results, current_results);

    Ok(Json(serde_json::json!({
        "baseline_run": RunView::from(baseline),
        "current_run": RunView::from(current),
        "items": items,
    })))
}

fn compute_compare(
    baseline: Vec<db::BenchmarkResult>,
    current: Vec<db::BenchmarkResult>,
) -> Vec<CompareItem> {
    let baseline_map: std::collections::HashMap<i64, db::BenchmarkResult> =
        baseline.into_iter().map(|r| (r.query_id, r)).collect();
    let current_map: std::collections::HashMap<i64, db::BenchmarkResult> =
        current.into_iter().map(|r| (r.query_id, r)).collect();

    let mut all_ids: std::collections::BTreeSet<i64> =
        baseline_map.keys().copied().collect();
    all_ids.extend(current_map.keys().copied());

    all_ids
        .into_iter()
        .map(|query_id| {
            let b = baseline_map.get(&query_id);
            let c = current_map.get(&query_id);
            let (added, removed) = diff_top_movies(
                b.map(|x| x.top_movie_ids.as_str()),
                c.map(|x| x.top_movie_ids.as_str()),
            );
            let intent_changed = normalize_intent(b.and_then(|x| x.intent_json.as_deref()))
                != normalize_intent(c.and_then(|x| x.intent_json.as_deref()));

            let query_text = c
                .map(|x| x.query_snapshot.clone())
                .or_else(|| b.map(|x| x.query_snapshot.clone()))
                .unwrap_or_default();

            let expected_ids = decode_expected(
                c.and_then(|x| x.expected_ids.as_deref())
                    .or_else(|| b.and_then(|x| x.expected_ids.as_deref())),
            );

            let hit_delta = match (
                b.and_then(|x| x.hit),
                c.and_then(|x| x.hit),
            ) {
                (Some(bh), Some(ch)) => Some(ch - bh),
                _ => None,
            };

            CompareItem {
                query_id,
                query_text,
                expected_ids,
                baseline: b.cloned().map(result_to_view),
                current: c.cloned().map(result_to_view),
                added_movies: added,
                removed_movies: removed,
                intent_changed,
                hit_delta,
            }
        })
        .collect()
}

fn parse_movie_array(s: &str) -> Vec<serde_json::Value> {
    serde_json::from_str::<Vec<serde_json::Value>>(s).unwrap_or_default()
}

fn movie_tmdb_id(m: &serde_json::Value) -> Option<i64> {
    m.get("tmdb_id").and_then(|v| v.as_i64())
}

fn diff_top_movies(
    baseline: Option<&str>,
    current: Option<&str>,
) -> (Vec<serde_json::Value>, Vec<serde_json::Value>) {
    let b = baseline.map(parse_movie_array).unwrap_or_default();
    let c = current.map(parse_movie_array).unwrap_or_default();
    let b_ids: std::collections::HashSet<i64> =
        b.iter().filter_map(movie_tmdb_id).collect();
    let c_ids: std::collections::HashSet<i64> =
        c.iter().filter_map(movie_tmdb_id).collect();
    let added = c
        .iter()
        .filter(|m| movie_tmdb_id(m).map(|id| !b_ids.contains(&id)).unwrap_or(false))
        .cloned()
        .collect();
    let removed = b
        .iter()
        .filter(|m| movie_tmdb_id(m).map(|id| !c_ids.contains(&id)).unwrap_or(false))
        .cloned()
        .collect();
    (added, removed)
}

fn normalize_intent(raw: Option<&str>) -> String {
    match raw {
        None => String::new(),
        Some(s) => serde_json::from_str::<serde_json::Value>(s)
            .and_then(|v| serde_json::to_string(&v))
            .unwrap_or_else(|_| s.to_string()),
    }
}

async fn set_baseline(
    _user: RequireUser,
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let run = db::get_benchmark_run(&state.pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or((StatusCode::NOT_FOUND, "run not found".to_string()))?;
    if run.status != "done" {
        return Err((
            StatusCode::BAD_REQUEST,
            "only completed runs can be baseline".to_string(),
        ));
    }
    db::set_benchmark_run_as_baseline(&state.pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(serde_json::json!({ "message": "baseline set" })))
}

async fn cancel_run(
    _user: RequireUser,
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let affected = db::request_benchmark_run_cancel(&state.pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if affected == 0 {
        return Err((StatusCode::NOT_FOUND, "no running benchmark to cancel".to_string()));
    }
    Ok(Json(serde_json::json!({ "message": "cancel requested" })))
}

// ===== Scoring =====
//
// `not_expected_ids` 是硬否决（picks 含任意一个 → 整条 fail）。`coverage_ratio`
// 用 Recall@K 语义记录 expected 命中率：分母是 `min(expected.len(), K)`，避免
// expected 数量超过 K (=10) 时永远到不了 1.0。详见
// docs/specs/2026-04-28-benchmark-not-expected-design.md

const MAX_RESULTS: usize = 10;

fn score_benchmark_result(
    expected: &[i64],
    not_expected: &[i64],
    pick_set: &std::collections::HashSet<i64>,
    max_results: usize,
) -> (Option<bool>, Option<f64>) {
    if expected.is_empty() && not_expected.is_empty() {
        return (None, None);
    }
    let has_negative = not_expected.iter().any(|id| pick_set.contains(id));
    let covered = expected.iter().filter(|id| pick_set.contains(id)).count();
    let effective_expected = expected.len().min(max_results);
    let coverage_ratio = if effective_expected > 0 {
        Some(covered as f64 / effective_expected as f64)
    } else {
        None
    };
    let hit = if expected.is_empty() {
        Some(!has_negative)
    } else {
        Some(covered >= effective_expected && !has_negative)
    };
    (hit, coverage_ratio)
}

// ===== Worker =====

async fn execute_benchmark_run(
    pool: db::SqlitePool,
    llm: crate::llm::LlmClient,
    embedding_model: std::sync::Arc<crate::embedding::EmbeddingModel>,
    embedding_store: std::sync::Arc<crate::embedding::EmbeddingStore>,
    run_id: i64,
    queries: Vec<db::BenchmarkQuery>,
) {
    tracing::info!(run_id, total = queries.len(), "benchmark run started");
    let mut canceled = false;

    for q in queries {
        if db::is_benchmark_run_cancel_requested(&pool, run_id)
            .await
            .unwrap_or(false)
        {
            canceled = true;
            break;
        }

        let started = Instant::now();
        let mut capture = crate::api::recommend::SearchCapture::default();
        let result = crate::api::recommend::run_smart_search(
            &llm,
            &pool,
            &embedding_model,
            &embedding_store,
            &q.query,
            10,
            None,
            &[],
            Some(&mut capture),
        )
        .await;
        let elapsed_ms = started.elapsed().as_millis() as i64;

        match result {
            Ok(items) => {
                let top: Vec<serde_json::Value> = items
                    .iter()
                    .map(|it| {
                        serde_json::json!({
                            "tmdb_id": it.movie.tmdb_id,
                            "title": it.movie.title,
                            "in_library": it.in_library,
                        })
                    })
                    .collect();

                let expected: Vec<i64> = decode_expected(q.expected_ids.as_deref());
                let not_expected: Vec<i64> = decode_expected(q.not_expected_ids.as_deref());
                let pick_set: std::collections::HashSet<i64> =
                    items.iter().map(|it| it.movie.tmdb_id).collect();
                let (hit, coverage_ratio) =
                    score_benchmark_result(&expected, &not_expected, &pick_set, MAX_RESULTS);

                let top_str = serde_json::to_string(&top).unwrap_or_else(|_| "[]".into());
                let _ = db::insert_benchmark_result(
                    &pool,
                    run_id,
                    q.id,
                    &q.query,
                    q.expected_ids.as_deref(),
                    &top_str,
                    capture.intent_json.as_deref(),
                    hit,
                    Some(elapsed_ms),
                    None,
                    q.not_expected_ids.as_deref(),
                    coverage_ratio,
                )
                .await;
                // 计分：hit=Some(true) 记 pass，其他（miss 或无 expected）不记 pass/fail 调用失败才算 failed
                let (pass_d, fail_d) = if matches!(hit, Some(true)) { (1, 0) } else { (0, 0) };
                let _ = db::increment_benchmark_run_counters(&pool, run_id, pass_d, fail_d).await;
            }
            Err(e) => {
                tracing::warn!(run_id, query_id = q.id, error = e.as_str(), "benchmark query failed");
                let _ = db::insert_benchmark_result(
                    &pool,
                    run_id,
                    q.id,
                    &q.query,
                    q.expected_ids.as_deref(),
                    "[]",
                    capture.intent_json.as_deref(),
                    None,
                    Some(elapsed_ms),
                    Some(&e),
                    q.not_expected_ids.as_deref(),
                    None,
                )
                .await;
                let _ = db::increment_benchmark_run_counters(&pool, run_id, 0, 1).await;
            }
        }
    }

    let final_status = if canceled { "canceled" } else { "done" };
    let _ = db::finalize_benchmark_run(&pool, run_id, final_status).await;
    tracing::info!(run_id, status = final_status, "benchmark run finished");
}

// ===== Aggregate: 同 prompt 历史搜索的 picks 去重 + 计数 =====
//
// `expected_ids` 维护是 admin 在 benchmark 详情页里挑选 "好答案" 的过程。
// 候选池来自所有同 prompt 的 search_history (跨用户跨时间)，用 `result` SSE
// 事件里的 recommendations 数组。设计文档：
// docs/specs/2026-04-28-benchmark-query-detail-aggregate-design.md

#[derive(Debug, Deserialize)]
struct AggregatePagination {
    #[serde(default = "default_aggregate_page")]
    page: i64,
    #[serde(default = "default_aggregate_page_size")]
    page_size: i64,
}

fn default_aggregate_page() -> i64 {
    1
}
fn default_aggregate_page_size() -> i64 {
    50
}

#[derive(Debug, Serialize)]
struct AggregateMovie {
    tmdb_id: i64,
    movie_id: Option<i64>,
    title: Option<String>,
    title_zh: Option<String>,
    title_en: Option<String>,
    poster_url: Option<String>,
    year: Option<i64>,
    appearance_count: i64,
    best_rank: Option<i64>,
    avg_rank: Option<f64>,
    latest_at: Option<String>,
    is_expected: bool,
    is_not_expected: bool,
}

#[derive(Debug, Serialize)]
struct AggregateResponse {
    query: QueryView,
    history_count: i64,
    total_movies: i64,
    page: i64,
    page_size: i64,
    movies: Vec<AggregateMovie>,
}

#[derive(Debug, Default)]
struct AggregateAccumulator {
    appearance_count: i64,
    best_rank: Option<i64>,
    rank_sum: i64,
    rank_count: i64,
    latest_at: Option<String>,
}

/// Walk a single `sse_events` JSON blob and yield (tmdb_id, rank) pairs from
/// the final `result` event. Returns empty vec on parse error so one bad row
/// doesn't poison the aggregation.
fn extract_picks_from_sse_events(sse_events_raw: &str) -> Vec<(i64, i64)> {
    let parsed: serde_json::Value = match serde_json::from_str(sse_events_raw) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    let arr = match parsed.as_array() {
        Some(a) => a,
        None => return Vec::new(),
    };
    let mut last_recs: Option<&serde_json::Value> = None;
    for ev in arr {
        if ev.get("event").and_then(|e| e.as_str()) == Some("result") {
            if let Some(recs) = ev
                .get("data")
                .and_then(|d| d.get("recommendations"))
                .filter(|r| r.is_array())
            {
                last_recs = Some(recs);
            }
        }
    }
    let recs = match last_recs.and_then(|r| r.as_array()) {
        Some(a) => a,
        None => return Vec::new(),
    };
    recs.iter()
        .enumerate()
        .filter_map(|(idx, item)| {
            let tmdb_id = item
                .get("movie")
                .and_then(|m| m.get("tmdb_id"))
                .and_then(|v| v.as_i64())?;
            // rank is 1-based for human readability.
            Some((tmdb_id, (idx as i64) + 1))
        })
        .collect()
}

/// Aggregate picks across all histories matching `prompt`. Returns sorted
/// movies (is_expected DESC, appearance_count DESC, avg_rank ASC, tmdb_id ASC)
/// and the count of histories actually used for stats.
fn aggregate_history_picks(
    histories: &[db::SearchHistoryDetail],
    expected_ids: &[i64],
) -> (Vec<(i64, AggregateAccumulator)>, i64) {
    use std::collections::HashMap;
    let mut acc: HashMap<i64, AggregateAccumulator> = HashMap::new();
    // histories is ordered created_at DESC; iterate that order so the first
    // observed latest_at per tmdb_id is the most recent.
    for h in histories {
        for (tmdb_id, rank) in extract_picks_from_sse_events(&h.sse_events) {
            let entry = acc.entry(tmdb_id).or_default();
            entry.appearance_count += 1;
            entry.best_rank = Some(match entry.best_rank {
                Some(prev) => prev.min(rank),
                None => rank,
            });
            entry.rank_sum += rank;
            entry.rank_count += 1;
            if entry.latest_at.is_none() {
                entry.latest_at = Some(h.created_at.clone());
            }
        }
    }
    // Inject expected_ids that never appeared so admins can audit / unset them.
    for &id in expected_ids {
        acc.entry(id).or_default();
    }

    let history_count = histories.len() as i64;

    let expected_set: std::collections::HashSet<i64> = expected_ids.iter().copied().collect();
    let mut sorted: Vec<(i64, AggregateAccumulator)> = acc.into_iter().collect();
    sorted.sort_by(|a, b| {
        let a_exp = expected_set.contains(&a.0);
        let b_exp = expected_set.contains(&b.0);
        b_exp.cmp(&a_exp)
            .then_with(|| b.1.appearance_count.cmp(&a.1.appearance_count))
            .then_with(|| {
                let a_avg = if a.1.rank_count > 0 {
                    a.1.rank_sum as f64 / a.1.rank_count as f64
                } else {
                    f64::INFINITY
                };
                let b_avg = if b.1.rank_count > 0 {
                    b.1.rank_sum as f64 / b.1.rank_count as f64
                } else {
                    f64::INFINITY
                };
                a_avg
                    .partial_cmp(&b_avg)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| a.0.cmp(&b.0))
    });
    (sorted, history_count)
}

async fn get_query_aggregate(
    _user: RequireUser,
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Query(params): Query<AggregatePagination>,
) -> Result<Json<AggregateResponse>, (StatusCode, String)> {
    let page = params.page.max(1);
    let page_size = params.page_size.clamp(1, 200);

    let bq = db::get_benchmark_query(&state.pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or((StatusCode::NOT_FOUND, "query not found".to_string()))?;
    let expected_ids = decode_expected(bq.expected_ids.as_deref());
    let not_expected_ids = decode_expected(bq.not_expected_ids.as_deref());

    let histories = db::list_search_history_by_prompt(&state.pool, &bq.query)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Inject both expected and not_expected so admins can audit/unset them.
    let pinned: Vec<i64> = expected_ids
        .iter()
        .chain(not_expected_ids.iter())
        .copied()
        .collect();
    let (sorted, history_count) = aggregate_history_picks(&histories, &pinned);
    let total_movies = sorted.len() as i64;

    // Slice for current page before hitting movies table — we only need
    // metadata for what the page renders.
    let start = ((page - 1) * page_size).max(0) as usize;
    let end = (start + page_size as usize).min(sorted.len());
    let page_slice: &[(i64, AggregateAccumulator)] =
        if start >= sorted.len() { &[] } else { &sorted[start..end] };

    let tmdb_ids: Vec<i64> = page_slice.iter().map(|(id, _)| *id).collect();
    let movies_map = db::get_movies_by_tmdb_ids(&state.pool, &tmdb_ids)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let expected_set: std::collections::HashSet<i64> = expected_ids.iter().copied().collect();
    let not_expected_set: std::collections::HashSet<i64> =
        not_expected_ids.iter().copied().collect();

    let movies: Vec<AggregateMovie> = page_slice
        .iter()
        .map(|(tmdb_id, agg)| {
            let movie = movies_map.get(tmdb_id);
            let avg_rank = if agg.rank_count > 0 {
                Some(agg.rank_sum as f64 / agg.rank_count as f64)
            } else {
                None
            };
            AggregateMovie {
                tmdb_id: *tmdb_id,
                movie_id: movie.map(|m| m.id),
                title: movie.map(|m| m.title.clone()),
                title_zh: movie.and_then(|m| m.title_zh.clone()),
                title_en: movie.and_then(|m| m.title_en.clone()),
                poster_url: movie.and_then(|m| m.poster_url.clone()),
                year: movie.and_then(|m| m.year),
                appearance_count: agg.appearance_count,
                best_rank: agg.best_rank,
                avg_rank,
                latest_at: agg.latest_at.clone(),
                is_expected: expected_set.contains(tmdb_id),
                is_not_expected: not_expected_set.contains(tmdb_id),
            }
        })
        .collect();

    Ok(Json(AggregateResponse {
        query: QueryView::from(bq),
        history_count,
        total_movies,
        page,
        page_size,
        movies,
    }))
}

#[derive(Debug, Serialize)]
struct MovieAppearance {
    history_id: i64,
    rank: i64,
    created_at: String,
}

#[derive(Debug, Serialize)]
struct MovieAppearancesResponse {
    tmdb_id: i64,
    appearances: Vec<MovieAppearance>,
}

async fn get_query_movie_appearances(
    _user: RequireUser,
    State(state): State<AppState>,
    Path((id, tmdb_id)): Path<(i64, i64)>,
) -> Result<Json<MovieAppearancesResponse>, (StatusCode, String)> {
    let bq = db::get_benchmark_query(&state.pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or((StatusCode::NOT_FOUND, "query not found".to_string()))?;

    let histories = db::list_search_history_by_prompt(&state.pool, &bq.query)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let mut appearances = Vec::new();
    for h in &histories {
        for (pid, rank) in extract_picks_from_sse_events(&h.sse_events) {
            if pid == tmdb_id {
                appearances.push(MovieAppearance {
                    history_id: h.id,
                    rank,
                    created_at: h.created_at.clone(),
                });
            }
        }
    }

    Ok(Json(MovieAppearancesResponse {
        tmdb_id,
        appearances,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::test_support::{get_json, post_json, test_app};
    use serde_json::json;
    use sqlx::SqlitePool;

    async fn register(pool: &SqlitePool, username: &str) -> String {
        let (_, body) = post_json(
            test_app(pool.clone()),
            "/api/auth/register",
            &json!({ "username": username, "password": "pw" }),
            None,
        )
        .await;
        body["token"].as_str().unwrap().to_string()
    }

    fn make_result(query_id: i64, ids: &[i64], intent: Option<&str>, hit: Option<i64>) -> db::BenchmarkResult {
        let top = ids
            .iter()
            .map(|id| serde_json::json!({ "tmdb_id": id, "title": format!("M{}", id) }))
            .collect::<Vec<_>>();
        db::BenchmarkResult {
            id: 0,
            run_id: 0,
            query_id,
            query_snapshot: format!("q{}", query_id),
            expected_ids: None,
            top_movie_ids: serde_json::to_string(&top).unwrap(),
            intent_json: intent.map(|s| s.to_string()),
            hit,
            elapsed_ms: Some(1),
            error: None,
            not_expected_ids: None,
            coverage_ratio: None,
        }
    }

    // ===== score_benchmark_result tests =====

    fn picks(ids: &[i64]) -> std::collections::HashSet<i64> {
        ids.iter().copied().collect()
    }

    #[test]
    fn score_returns_null_when_no_expected_no_negative() {
        let (hit, cov) = score_benchmark_result(&[], &[], &picks(&[1, 2, 3]), 10);
        assert_eq!(hit, None);
        assert_eq!(cov, None);
    }

    #[test]
    fn score_pass_when_all_expected_covered() {
        let (hit, cov) = score_benchmark_result(&[1, 2, 3], &[], &picks(&[1, 2, 3, 99]), 10);
        assert_eq!(hit, Some(true));
        assert_eq!(cov, Some(1.0));
    }

    #[test]
    fn score_fail_when_partially_covered() {
        let (hit, cov) = score_benchmark_result(&[1, 2, 3], &[], &picks(&[1, 99]), 10);
        assert_eq!(hit, Some(false));
        let c = cov.unwrap();
        assert!((c - 1.0 / 3.0).abs() < 1e-6);
    }

    #[test]
    fn score_not_expected_hard_veto_even_when_all_covered() {
        let (hit, cov) =
            score_benchmark_result(&[1, 2, 3], &[99], &picks(&[1, 2, 3, 99]), 10);
        assert_eq!(hit, Some(false));
        assert_eq!(cov, Some(1.0)); // coverage 仍是 1.0，但 hit 被否决
    }

    #[test]
    fn score_pure_negative_no_expected() {
        // 只配 not_expected：只要 picks 不含就 pass
        let (hit, cov) = score_benchmark_result(&[], &[99], &picks(&[1, 2, 3]), 10);
        assert_eq!(hit, Some(true));
        assert_eq!(cov, None);
        let (hit2, _) = score_benchmark_result(&[], &[99], &picks(&[1, 99]), 10);
        assert_eq!(hit2, Some(false));
    }

    #[test]
    fn score_recall_at_k_caps_denominator_at_max_results() {
        // expected 20 个，max_results=10，10 个全在 picks 里 → 1.0 pass
        let expected: Vec<i64> = (1..=20).collect();
        let pick_set = picks(&(1..=10).collect::<Vec<i64>>());
        let (hit, cov) = score_benchmark_result(&expected, &[], &pick_set, 10);
        assert_eq!(hit, Some(true));
        assert_eq!(cov, Some(1.0));
    }

    #[test]
    fn score_recall_at_k_partial_with_oversize_expected() {
        // expected 20 个，命中 5 个 → 5/min(20,10) = 0.5
        let expected: Vec<i64> = (1..=20).collect();
        let pick_set = picks(&[1, 2, 3, 4, 5]);
        let (hit, cov) = score_benchmark_result(&expected, &[], &pick_set, 10);
        assert_eq!(hit, Some(false));
        assert_eq!(cov, Some(0.5));
    }

    #[test]
    fn score_empty_picks_against_expected_is_fail() {
        let (hit, cov) = score_benchmark_result(&[1, 2], &[], &picks(&[]), 10);
        assert_eq!(hit, Some(false));
        assert_eq!(cov, Some(0.0));
    }

    #[test]
    fn score_zero_coverage_with_negative_clean_still_fails() {
        // 没有 expected 命中 → fail，即使 not_expected 也没命中
        let (hit, cov) = score_benchmark_result(&[1], &[99], &picks(&[2]), 10);
        assert_eq!(hit, Some(false));
        assert_eq!(cov, Some(0.0));
    }

    #[test]
    fn compute_compare_detects_added_and_removed() {
        let baseline = vec![make_result(1, &[100, 200, 300], Some(r#"{"a":1}"#), Some(1))];
        let current = vec![make_result(1, &[100, 200, 400], Some(r#"{"a":1}"#), Some(1))];
        let items = compute_compare(baseline, current);
        assert_eq!(items.len(), 1);
        let it = &items[0];
        assert_eq!(it.added_movies.len(), 1);
        assert_eq!(movie_tmdb_id(&it.added_movies[0]), Some(400));
        assert_eq!(it.removed_movies.len(), 1);
        assert_eq!(movie_tmdb_id(&it.removed_movies[0]), Some(300));
        assert!(!it.intent_changed);
        assert_eq!(it.hit_delta, Some(0));
    }

    #[test]
    fn compute_compare_flags_intent_change() {
        let baseline = vec![make_result(1, &[100], Some(r#"{"a":1}"#), None)];
        let current = vec![make_result(1, &[100], Some(r#"{"a":2}"#), None)];
        let items = compute_compare(baseline, current);
        assert!(items[0].intent_changed);
    }

    #[test]
    fn compute_compare_handles_missing_side() {
        let baseline = vec![make_result(1, &[100], None, None)];
        let current = vec![make_result(2, &[200], None, None)];
        let items = compute_compare(baseline, current);
        // Both query_ids present
        assert_eq!(items.len(), 2);
        // query 1 only has baseline
        let q1 = items.iter().find(|i| i.query_id == 1).unwrap();
        assert!(q1.current.is_none());
        assert!(q1.baseline.is_some());
        // query 2 only has current
        let q2 = items.iter().find(|i| i.query_id == 2).unwrap();
        assert!(q2.baseline.is_none());
        assert!(q2.current.is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn baseline_switch_clears_previous(pool: SqlitePool) {
        // Seed two runs
        let r1 = db::insert_benchmark_run(&pool, 0, None).await.unwrap();
        db::finalize_benchmark_run(&pool, r1, "done").await.unwrap();
        let r2 = db::insert_benchmark_run(&pool, 0, None).await.unwrap();
        db::finalize_benchmark_run(&pool, r2, "done").await.unwrap();

        db::set_benchmark_run_as_baseline(&pool, r1).await.unwrap();
        let b1 = db::get_baseline_benchmark_run(&pool).await.unwrap().unwrap();
        assert_eq!(b1.id, r1);

        db::set_benchmark_run_as_baseline(&pool, r2).await.unwrap();
        let b2 = db::get_baseline_benchmark_run(&pool).await.unwrap().unwrap();
        assert_eq!(b2.id, r2);

        let r1_after = db::get_benchmark_run(&pool, r1).await.unwrap().unwrap();
        assert_eq!(r1_after.is_baseline, 0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn running_run_blocks_start(pool: SqlitePool) {
        // No running -> None
        assert!(db::get_running_benchmark_run(&pool).await.unwrap().is_none());

        let _ = db::insert_benchmark_run(&pool, 5, Some("test")).await.unwrap();
        let running = db::get_running_benchmark_run(&pool).await.unwrap();
        assert!(running.is_some());
        assert_eq!(running.unwrap().status, "running");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn queries_crud_roundtrip(pool: SqlitePool) {
        let id = db::insert_benchmark_query(
            &pool,
            "黑帮史诗",
            Some("经典回归"),
            Some("[238,240]"),
            None,
            None,
        )
        .await
        .unwrap();
        let rows = db::list_benchmark_queries(&pool).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, id);
        assert_eq!(rows[0].expected_ids.as_deref(), Some("[238,240]"));

        db::update_benchmark_query(&pool, id, "黑帮史诗2", Some("n2"), None, None)
            .await
            .unwrap();
        let rows2 = db::list_benchmark_queries(&pool).await.unwrap();
        assert_eq!(rows2[0].query, "黑帮史诗2");
        assert!(rows2[0].expected_ids.is_none());

        let affected = db::delete_benchmark_query(&pool, id).await.unwrap();
        assert_eq!(affected, 1);
        assert!(db::list_benchmark_queries(&pool).await.unwrap().is_empty());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn create_query_persists_source_history_id(pool: SqlitePool) {
        let token = register(&pool, "alice").await;
        let history_id = db::insert_search_history(&pool, 1, "q", "[]", 0)
            .await
            .unwrap();

        let (status, body) = post_json(
            test_app(pool.clone()),
            "/api/benchmark/queries",
            &json!({
                "query": "世界名著改编的电影",
                "expected_ids": [770, 1084736],
                "source_history_id": history_id,
            }),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let id = body["id"].as_i64().unwrap();

        let (_, list_body) = get_json(
            test_app(pool.clone()),
            "/api/benchmark/queries",
            Some(&token),
        )
        .await;
        let saved = list_body
            .as_array()
            .unwrap()
            .iter()
            .find(|q| q["id"].as_i64() == Some(id))
            .expect("saved query in list");
        assert_eq!(saved["source_history_id"].as_i64(), Some(history_id));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_query_runs_returns_history_chronological_desc(pool: SqlitePool) {
        let token = register(&pool, "alice").await;
        let query_id = db::insert_benchmark_query(&pool, "test", None, None, None, None)
            .await
            .unwrap();

        let run_old: i64 = sqlx::query_scalar(
            "INSERT INTO benchmark_runs (started_at, status, total, passed, failed) \
             VALUES ('2026-04-26 10:00:00', 'done', 1, 1, 0) RETURNING id",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let run_new: i64 = sqlx::query_scalar(
            "INSERT INTO benchmark_runs (started_at, status, total, passed, failed) \
             VALUES ('2026-04-26 12:00:00', 'done', 1, 0, 1) RETURNING id",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO benchmark_results \
                (run_id, query_id, query_snapshot, top_movie_ids, hit, elapsed_ms) \
             VALUES (?, ?, 'test', '[]', 1, 100), (?, ?, 'test', '[]', 0, 200)",
        )
        .bind(run_old)
        .bind(query_id)
        .bind(run_new)
        .bind(query_id)
        .execute(&pool)
        .await
        .unwrap();

        let (status, body) = get_json(
            test_app(pool.clone()),
            &format!("/api/benchmark/queries/{}/runs", query_id),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let arr = body.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["run_id"].as_i64(), Some(run_new));
        assert_eq!(arr[0]["hit"].as_bool(), Some(false));
        assert_eq!(arr[1]["run_id"].as_i64(), Some(run_old));
        assert_eq!(arr[1]["hit"].as_bool(), Some(true));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_query_runs_404_for_nonexistent_query(pool: SqlitePool) {
        let token = register(&pool, "alice").await;
        let (status, _) = get_json(
            test_app(pool),
            "/api/benchmark/queries/9999/runs",
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_query_runs_returns_empty_for_query_with_no_runs(pool: SqlitePool) {
        let token = register(&pool, "alice").await;
        let query_id = db::insert_benchmark_query(&pool, "test", None, None, None, None)
            .await
            .unwrap();

        let (status, body) = get_json(
            test_app(pool),
            &format!("/api/benchmark/queries/{}/runs", query_id),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.as_array().unwrap().len(), 0);
    }

    // ===== Aggregate tests =====

    fn make_sse_events(picks: &[i64]) -> String {
        let recs: Vec<serde_json::Value> = picks
            .iter()
            .map(|id| serde_json::json!({ "movie": { "tmdb_id": id } }))
            .collect();
        serde_json::to_string(&serde_json::json!([
            { "event": "thinking", "data": { "stage": "ranking" } },
            { "event": "result", "data": { "recommendations": recs } },
        ]))
        .unwrap()
    }

    fn make_history(id: i64, sse_events: &str, created_at: &str) -> db::SearchHistoryDetail {
        db::SearchHistoryDetail {
            id,
            prompt: "p".to_string(),
            sse_events: sse_events.to_string(),
            result_count: 0,
            created_at: created_at.to_string(),
            share_token: None,
        }
    }

    #[test]
    fn extract_picks_handles_normal_result() {
        let sse = make_sse_events(&[100, 200, 300]);
        let picks = extract_picks_from_sse_events(&sse);
        assert_eq!(picks, vec![(100, 1), (200, 2), (300, 3)]);
    }

    #[test]
    fn extract_picks_returns_empty_on_bad_json() {
        assert!(extract_picks_from_sse_events("not json").is_empty());
    }

    #[test]
    fn extract_picks_returns_empty_when_no_result_event() {
        let sse = serde_json::to_string(&serde_json::json!([
            { "event": "thinking", "data": { "stage": "recall" } }
        ]))
        .unwrap();
        assert!(extract_picks_from_sse_events(&sse).is_empty());
    }

    #[test]
    fn aggregate_counts_appearances_and_keeps_best_rank() {
        // 历史 1 (latest):  [100, 200, 300]
        // 历史 2 (older):   [200, 100]   ← 200 在 #1, 100 在 #2
        let h1 = make_history(1, &make_sse_events(&[100, 200, 300]), "2026-04-28T00:00:00");
        let h2 = make_history(2, &make_sse_events(&[200, 100]), "2026-04-27T00:00:00");
        let (sorted, count) = aggregate_history_picks(&[h1, h2], &[]);
        assert_eq!(count, 2);
        // 100 appears twice (rank 1 + rank 2 → best=1, avg=1.5)
        // 200 appears twice (rank 2 + rank 1 → best=1, avg=1.5)
        // 300 appears once  (rank 3)
        // Sort: appearance_count DESC, then avg ASC → 100 vs 200 tied on count + avg, then tmdb_id ASC.
        let by_id: std::collections::HashMap<i64, &AggregateAccumulator> =
            sorted.iter().map(|(k, v)| (*k, v)).collect();
        assert_eq!(by_id[&100].appearance_count, 2);
        assert_eq!(by_id[&100].best_rank, Some(1));
        assert_eq!(by_id[&100].rank_sum, 3);
        assert_eq!(by_id[&100].rank_count, 2);
        assert_eq!(by_id[&100].latest_at.as_deref(), Some("2026-04-28T00:00:00"));
        assert_eq!(by_id[&300].appearance_count, 1);
        assert_eq!(by_id[&300].best_rank, Some(3));
    }

    #[test]
    fn aggregate_includes_expected_ids_with_zero_appearances() {
        let h1 = make_history(1, &make_sse_events(&[100]), "2026-04-28T00:00:00");
        let (sorted, _) = aggregate_history_picks(&[h1], &[999]);
        let by_id: std::collections::HashMap<i64, &AggregateAccumulator> =
            sorted.iter().map(|(k, v)| (*k, v)).collect();
        assert!(by_id.contains_key(&999));
        assert_eq!(by_id[&999].appearance_count, 0);
        assert!(by_id[&999].best_rank.is_none());
        // Expected goes first.
        assert_eq!(sorted[0].0, 999);
    }

    #[test]
    fn aggregate_skips_bad_sse_without_crashing() {
        let h_bad = make_history(1, "not json", "2026-04-28T00:00:00");
        let h_good = make_history(2, &make_sse_events(&[100]), "2026-04-27T00:00:00");
        let (sorted, count) = aggregate_history_picks(&[h_bad, h_good], &[]);
        assert_eq!(count, 2); // history_count counts all, even bad
        assert_eq!(sorted.len(), 1);
        assert_eq!(sorted[0].0, 100);
    }

    #[test]
    fn aggregate_orders_expected_first_then_count_then_avg() {
        // expected=[1]; appearance: 1@1×2  2@1×3  3@1×1
        let h1 = make_history(1, &make_sse_events(&[1, 2, 3]), "t1");
        let h2 = make_history(2, &make_sse_events(&[2, 1]), "t2");
        let h3 = make_history(3, &make_sse_events(&[2]), "t3");
        let (sorted, _) = aggregate_history_picks(&[h1, h2, h3], &[1]);
        let order: Vec<i64> = sorted.iter().map(|(id, _)| *id).collect();
        // 1 first (expected), then 2 (count=3), then 3 (count=1)
        assert_eq!(order, vec![1, 2, 3]);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn aggregate_endpoint_returns_query_with_no_history(pool: SqlitePool) {
        let token = register(&pool, "alice").await;
        let query_id = db::insert_benchmark_query(&pool, "test query", None, None, None, None)
            .await
            .unwrap();

        let (status, body) = get_json(
            test_app(pool),
            &format!("/api/benchmark/queries/{}/aggregate", query_id),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["history_count"].as_i64(), Some(0));
        assert_eq!(body["total_movies"].as_i64(), Some(0));
        assert!(body["movies"].as_array().unwrap().is_empty());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn aggregate_endpoint_404_for_unknown_query(pool: SqlitePool) {
        let token = register(&pool, "alice").await;
        let (status, _) = get_json(
            test_app(pool),
            "/api/benchmark/queries/9999/aggregate",
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn aggregate_endpoint_aggregates_across_users(pool: SqlitePool) {
        let alice_token = register(&pool, "alice").await;
        let bob_token = register(&pool, "bob").await;
        // Get user ids from auth — register returns token; we need user_id for
        // insert_search_history. Pull them from DB.
        let alice_id: i64 = sqlx::query_scalar("SELECT id FROM users WHERE username='alice'")
            .fetch_one(&pool)
            .await
            .unwrap();
        let bob_id: i64 = sqlx::query_scalar("SELECT id FROM users WHERE username='bob'")
            .fetch_one(&pool)
            .await
            .unwrap();
        let _ = bob_token; // bob's token unused; we only verify cross-user aggregation in DB.

        let query_id =
            db::insert_benchmark_query(&pool, "popular query", None, Some("[300]"), None, None)
                .await
                .unwrap();

        // alice: [100, 200], bob: [200, 100, 400]
        db::insert_search_history(
            &pool,
            alice_id,
            "popular query",
            &make_sse_events(&[100, 200]),
            2,
        )
        .await
        .unwrap();
        db::insert_search_history(
            &pool,
            bob_id,
            "popular query",
            &make_sse_events(&[200, 100, 400]),
            3,
        )
        .await
        .unwrap();

        let (status, body) = get_json(
            test_app(pool),
            &format!("/api/benchmark/queries/{}/aggregate", query_id),
            Some(&alice_token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["history_count"].as_i64(), Some(2));
        // 4 distinct: 100, 200, 400 from picks + 300 from expected_ids
        assert_eq!(body["total_movies"].as_i64(), Some(4));
        let movies = body["movies"].as_array().unwrap();
        // Expected (300) first
        assert_eq!(movies[0]["tmdb_id"].as_i64(), Some(300));
        assert_eq!(movies[0]["is_expected"].as_bool(), Some(true));
        assert_eq!(movies[0]["appearance_count"].as_i64(), Some(0));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn create_query_rejects_overlapping_expected_and_not_expected(pool: SqlitePool) {
        let token = register(&pool, "alice").await;
        // post_json swallows the plain-text error body; status code alone is
        // contract enough since validate_expected_disjoint has its own unit
        // coverage via direct call.
        let (status, _) = post_json(
            test_app(pool),
            "/api/benchmark/queries",
            &json!({
                "query": "x",
                "expected_ids": [1, 2, 3],
                "not_expected_ids": [3, 99],
            }),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn validate_expected_disjoint_passes_when_disjoint() {
        let res = validate_expected_disjoint(&Some(vec![1, 2]), &Some(vec![3, 4]));
        assert!(res.is_ok());
    }

    #[test]
    fn validate_expected_disjoint_passes_when_either_empty() {
        assert!(validate_expected_disjoint(&Some(vec![1, 2]), &None).is_ok());
        assert!(validate_expected_disjoint(&None, &Some(vec![3, 4])).is_ok());
        assert!(validate_expected_disjoint(&Some(vec![]), &Some(vec![1])).is_ok());
    }

    #[test]
    fn validate_expected_disjoint_fails_when_overlap() {
        let res = validate_expected_disjoint(&Some(vec![1, 2, 3]), &Some(vec![3, 4]));
        let (code, msg) = res.unwrap_err();
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert!(msg.contains("overlap"));
        assert!(msg.contains('3'));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn create_query_round_trips_not_expected_ids(pool: SqlitePool) {
        let token = register(&pool, "alice").await;
        let (status, body) = post_json(
            test_app(pool.clone()),
            "/api/benchmark/queries",
            &json!({
                "query": "x",
                "expected_ids": [1, 2],
                "not_expected_ids": [99, 100],
            }),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let id = body["id"].as_i64().unwrap();
        let (_, list) = get_json(test_app(pool), "/api/benchmark/queries", Some(&token)).await;
        let q = list
            .as_array()
            .unwrap()
            .iter()
            .find(|q| q["id"].as_i64() == Some(id))
            .unwrap();
        let exp: Vec<i64> = q["expected_ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_i64().unwrap())
            .collect();
        let neg: Vec<i64> = q["not_expected_ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_i64().unwrap())
            .collect();
        assert_eq!(exp, vec![1, 2]);
        assert_eq!(neg, vec![99, 100]);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn movie_appearances_endpoint_returns_history_breakdown(pool: SqlitePool) {
        let token = register(&pool, "alice").await;
        let alice_id: i64 = sqlx::query_scalar("SELECT id FROM users WHERE username='alice'")
            .fetch_one(&pool)
            .await
            .unwrap();
        let query_id = db::insert_benchmark_query(&pool, "qx", None, None, None, None)
            .await
            .unwrap();
        let h1 = db::insert_search_history(&pool, alice_id, "qx", &make_sse_events(&[100, 200]), 2)
            .await
            .unwrap();
        let h2 = db::insert_search_history(&pool, alice_id, "qx", &make_sse_events(&[300, 100]), 2)
            .await
            .unwrap();

        let (status, body) = get_json(
            test_app(pool),
            &format!(
                "/api/benchmark/queries/{}/aggregate/movies/{}",
                query_id, 100
            ),
            Some(&token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["tmdb_id"].as_i64(), Some(100));
        let arr = body["appearances"].as_array().unwrap();
        assert_eq!(arr.len(), 2);
        // history list order is created_at DESC → h2 first
        let ids: Vec<i64> = arr
            .iter()
            .map(|a| a["history_id"].as_i64().unwrap())
            .collect();
        assert!(ids.contains(&h1));
        assert!(ids.contains(&h2));
    }
}
