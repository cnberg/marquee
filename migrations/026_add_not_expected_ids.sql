-- Benchmark: 「不应包含」标准答案 + Recall@K 细粒度评分
--
-- benchmark_queries.not_expected_ids: 与 expected_ids 镜像，JSON 数组 of TMDB ids
-- benchmark_results.not_expected_ids: 运行时快照（与 expected_ids 平行）
-- benchmark_results.coverage_ratio: Recall@K 命中率，分母 min(expected.len(), 10)

ALTER TABLE benchmark_queries ADD COLUMN not_expected_ids TEXT;
ALTER TABLE benchmark_results ADD COLUMN not_expected_ids TEXT;
ALTER TABLE benchmark_results ADD COLUMN coverage_ratio REAL;
