-- 豆瓣 CSV 导入记录：每条对应一行豆瓣 subject。
-- status 流转：pending → matched / created / skipped / failed
-- - matched: TMDB 命中且库内已有 movie，已写 user_movie_marks
-- - created: TMDB 命中但库内无 movie，新建 movies 行（source='related'），抓全量元数据后写 marks
-- - pending: TMDB 找不到（含剧集），等用户在"豆瓣待绑定"页手工绑或 skip
-- - skipped: 用户标记跳过
-- - failed: 处理时出错（TMDB 限速 / 网络 / 解析），可重试
CREATE TABLE douban_imports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id),
    douban_subject_id TEXT NOT NULL,
    raw_title TEXT NOT NULL,
    parsed_title_zh TEXT,
    parsed_title_en TEXT,
    year INTEGER,
    country TEXT,
    douban_url TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending', 'matched', 'created', 'skipped', 'failed')),
    movie_id INTEGER REFERENCES movies(id),
    error_msg TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(user_id, douban_subject_id)
);

CREATE INDEX idx_douban_imports_status ON douban_imports(user_id, status);
CREATE INDEX idx_douban_imports_movie ON douban_imports(movie_id);
