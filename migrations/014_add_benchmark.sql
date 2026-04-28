-- Benchmark 回归测试系统
--
-- 设计：所有 query 与运行结果落库，不写入代码仓，避免公开发布时泄露。
-- 参见 docs/specs/benchmark.md

CREATE TABLE IF NOT EXISTS benchmark_queries (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    query        TEXT NOT NULL,
    note         TEXT,
    expected_ids TEXT,                       -- JSON array of TMDB ids; NULL 表示不评 hit
    created_at   TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS benchmark_runs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at       TEXT NOT NULL,
    finished_at      TEXT,
    status           TEXT NOT NULL,          -- running / done / error / canceled
    total            INTEGER NOT NULL DEFAULT 0,
    passed           INTEGER NOT NULL DEFAULT 0,
    failed           INTEGER NOT NULL DEFAULT 0,
    note             TEXT,
    is_baseline      INTEGER NOT NULL DEFAULT 0,
    cancel_requested INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_benchmark_runs_status ON benchmark_runs(status);

CREATE TABLE IF NOT EXISTS benchmark_results (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id         INTEGER NOT NULL REFERENCES benchmark_runs(id) ON DELETE CASCADE,
    query_id       INTEGER NOT NULL REFERENCES benchmark_queries(id) ON DELETE CASCADE,
    query_snapshot TEXT NOT NULL,            -- 运行时的 query 文本（query 行被改/删后仍可追溯）
    expected_ids   TEXT,                     -- 同上
    top_movie_ids  TEXT NOT NULL,            -- JSON: [{"tmdb_id":238,"title":"教父"}, ...]
    intent_json    TEXT,                     -- query-understand 输出的 intent 结构（字符串化 JSON）
    hit            INTEGER,                  -- 0/1/NULL（无 expected 则 NULL）
    elapsed_ms     INTEGER,
    error          TEXT
);
CREATE INDEX IF NOT EXISTS idx_benchmark_results_run ON benchmark_results(run_id);
CREATE INDEX IF NOT EXISTS idx_benchmark_results_query ON benchmark_results(query_id);
