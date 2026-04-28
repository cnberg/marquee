-- 标记 benchmark query 的来源——若来自一条 search_history（用户在首页搜过且之后想纳入回归），
-- 记录那条 search_history.id。手动输入的 query 此列为 NULL。
ALTER TABLE benchmark_queries
  ADD COLUMN source_history_id INTEGER REFERENCES search_history(id);

CREATE INDEX idx_benchmark_queries_source
  ON benchmark_queries(source_history_id);
