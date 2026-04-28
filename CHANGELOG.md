# Changelog

All notable changes to Marquee are documented in this file.
Format inspired by [Keep a Changelog](https://keepachangelog.com/).

## [v0.2.0] - 2026-04-28

### ✨ 新功能
- Prompt 回归测试套件（管理后台 `/admin/benchmark`）：题库管理 + 批量跑 + 与基线 run 逐题对比 + 跨用户历史聚合帮 admin 挑 expected_ids + Recall@K 评分 + not_expected_ids（false positive 维度）
- 多目录扫描 + SSH 远端扫描：`scan.movie_dirs` 改为数组，支持本地路径 + `ssh://user@host/path` 混挂；远端通过 `ssh` 命令读目录列表，无需把片子拷到本机
- qBittorrent 集成：读取下载完成的种子列表，回填文件大小、媒体类型到对应电影；详情页直接显示哪些种子在跑、各自多大；后台轮询同步
- 豆瓣 CSV 导入：管理后台 `/admin/import-douban` 上传 [DouBanExport](https://github.com/UlyC/DouBanExport) 浏览器扩展导出的"看过"列表；按 TMDB 限速逐条匹配，库内已有打"看过"标记，库外自动新建并抓元数据，未自动确认的进同页"待绑定"列表手动选 TMDB
- 反向定位：详情页"在硬盘里找一下"按钮，在已扫描但未匹配的目录里按片名年份打分，让你挑回正确路径绑定
- 多版本影片管理：管理后台 `/admin/multi-version` 集中处理同一部影片对应多个文件版本（不同分辨率 / 不同剪辑）
- 人物推荐路径：自然语言里的"诺兰的电影"、"梁朝伟主演"自动走 person-pick LLM，给每部电影差异化推荐语
- 搜索历史分享：登录用户主动 opt-in 把一次搜索历史变成公开 `/api/shared/:token` 链接（无需登录可看），并可随时撤销
- 移动端响应式：顶部菜单改为左侧抽屉（基于 radix-ui dialog），桌面端保持水平栏不变
- 灵感推荐 + 每日推荐：首页系统主动提议；每日推荐按日期缓存
- 影片标记主页：登录用户的"看过 / 想看 / 收藏"分组浏览页（`/marks`，Tab 切换 + 排序）

### ♻️ 改动
- TMDB keyword 中文化：runtime 内存字典 `(en, zh)` + 后台 LLM 翻译 worker，索引时把 keyword 替换成中文短语，显著改善中文 query 的语义召回（BGE-Small-ZH 单语模型对英文 token 信号弱）
- TMDB overview 中文化：5371 部库外影片有英文 overview 但 TMDB 没翻译 zh，新增 LLM 翻译 worker 自动补；TMDB 重抓时智能比较防止覆盖 LLM 译版
- Embedding 漂移检测：周期性比对已存向量的 embedding 文本与当前 `build_embedding_text` 输出，不一致就重 embed
- 推荐召回新增第 4 路：用户原 prompt 直接 embedding 召回，捕获 LLM 改写后丢失的语义
- Ranking rating 维度改用 Bayesian 加权：1 票 10.0 的边缘片不再淹没真正高质量电影
- 远程编译部署通道：从本机编译 + scp 196M 二进制，切到 `git push runtime` 触发 runtime hook 上 cargo build + 替换二进制；慢网下从 30+ 分钟降到几 KB diff
- person-pick prompt 精简：删禁令清单、把魔数并入精简评测

### 🐛 修复
- recent-library 冷查询：5s → 10ms（加索引 + 改聚合）
- most-related cache：通过 `dir_movie_mappings.MAX(updated_at)` 快照自动失效，无需 mutation 路径手动清
- 推荐 cosine 公式：LanceDB 返回平方 L2 距离，之前算成普通 L2 导致语义分错位；同时改用绝对 cosine + floor，扔掉 per-batch 归一化
- structured_recall：池随 saturation 收缩，constraints 全空时降到合理大小，避免低相关候选稀释
- smart-rank：LLM reason 内未转义双引号引发解析失败 → 宽松 parser
- daily-picks：rerank fallback / partial 不留模板 reason；person 路径模板 reason 全相同检测 → 调 LLM 重生成
- in_library 判定：全部走 `dir_movie_mappings` ground truth，不再依赖 `movies.source`（first-touch 标记）
- history-share：自动复制剪贴板 + Firefox HTTP 下 fallback 兼容
- query-understand zh prompt：强制 search_intents 用中文写，避免被 LLM 偷懒填英文
- person 路径修饰词丢弃：「成龙早期的电影」不再返回 2025 年新片
- admin 白屏：非 HTTPS 环境下 `crypto.randomUUID` undefined 兼容
- most-related LLM 推荐语异步生成，不阻塞首页响应

### 📝 文档
- 模块文档持续维护（recommendation / scanning-and-matching / web-ui / deployment-and-ops）
- specs 设计文档积累（benchmark detail page / multi-version admin / not_expected_ids / keyword 中文化等）
