// Stage 0 查询分类器。详见 docs/specs/query-router.md +
// docs/specs/2026-04-25-query-classifier-subject-design.md。
//
// 输出 schema：{type, subject:{name, kind}, confidence, reasoning}
// subject 是多态的（kind = movie / person / movement / studio / franchise），
// 让 similar_to 能接受任意"参考物"作为种子。
//
// 解析层兜了向后兼容：旧 schema {type, reference, reference_person, ...} 仍能
// parse，会按 type + 字段非空情况构造对应 Subject。这条路径专为 history /
// benchmark 历史 intent_json 的只读展示存在，不写新数据。

use serde::{Deserialize, Serialize, Serializer};

use crate::db::{self, SqlitePool};
use crate::llm::LlmClient;

pub const DEFAULT_CONFIDENCE_THRESHOLD: f32 = 0.6;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryKind {
    ExactTitle,
    SimilarTo,
    Person,
    Attribute,
    Descriptive,
}

impl QueryKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            QueryKind::ExactTitle => "exact_title",
            QueryKind::SimilarTo => "similar_to",
            QueryKind::Person => "person",
            QueryKind::Attribute => "attribute",
            QueryKind::Descriptive => "descriptive",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubjectKind {
    Movie,
    Person,
    Movement,
    Studio,
    Franchise,
}

impl SubjectKind {
    fn parse(s: &str) -> Option<Self> {
        match s.trim() {
            "movie" => Some(SubjectKind::Movie),
            "person" => Some(SubjectKind::Person),
            "movement" => Some(SubjectKind::Movement),
            "studio" => Some(SubjectKind::Studio),
            "franchise" => Some(SubjectKind::Franchise),
            _ => None,
        }
    }
}

/// 查询参考物——多态（电影 / 人 / 流派 / 厂牌 / 系列）。
/// `kind` 是鉴别位，handler 按 kind 分流处理。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subject {
    pub name: String,
    pub kind: SubjectKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryClassification {
    pub kind: QueryKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<Subject>,
    #[serde(serialize_with = "serialize_confidence_rounded")]
    pub confidence: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<String>,
}

fn serialize_confidence_rounded<S: Serializer>(v: &f32, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_f32((v * 100.0).round() / 100.0)
}

impl QueryClassification {
    /// 低置信度或解析失败时的回退，保证下游永远能拿到一个分类结果。
    pub fn fallback_descriptive(reason: impl Into<String>) -> Self {
        QueryClassification {
            kind: QueryKind::Descriptive,
            subject: None,
            confidence: 0.0,
            reasoning: Some(reason.into()),
        }
    }

    /// 置信度不够 / subject 缺失 / subject.kind 与 type 不匹配时强制走 descriptive。
    pub fn normalized(self, threshold: f32) -> Self {
        if self.confidence < threshold {
            return Self::fallback_descriptive(format!(
                "confidence {:.2} below threshold {:.2}; downgrade to descriptive",
                self.confidence, threshold
            ));
        }
        match self.kind {
            QueryKind::ExactTitle => match self.subject.as_ref() {
                None => Self::fallback_descriptive("missing subject for exact_title; downgrade"),
                Some(s) if !matches!(s.kind, SubjectKind::Movie) => Self::fallback_descriptive(
                    format!("exact_title requires subject.kind=movie, got {:?}", s.kind),
                ),
                Some(s) if s.name.trim().is_empty() => {
                    Self::fallback_descriptive("empty subject.name; downgrade")
                }
                _ => self,
            },
            QueryKind::SimilarTo => match self.subject.as_ref() {
                None => Self::fallback_descriptive("missing subject for similar_to; downgrade"),
                Some(s) if s.name.trim().is_empty() => {
                    Self::fallback_descriptive("empty subject.name; downgrade")
                }
                _ => self,
            },
            QueryKind::Person => match self.subject.as_ref() {
                None => Self::fallback_descriptive("missing subject for person; downgrade"),
                Some(s) if !matches!(s.kind, SubjectKind::Person) => Self::fallback_descriptive(
                    format!("person requires subject.kind=person, got {:?}", s.kind),
                ),
                Some(s) if s.name.trim().is_empty() => {
                    Self::fallback_descriptive("empty subject.name; downgrade")
                }
                _ => self,
            },
            QueryKind::Attribute | QueryKind::Descriptive => self,
        }
    }
}

// LLM 返回的原始 JSON。同时支持新旧两种 schema：
//   新：{type, subject:{name, kind}, confidence, reasoning}
//   旧：{type, reference, reference_person, confidence, reasoning}
#[derive(Debug, Deserialize)]
struct RawClassification {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    subject: Option<RawSubject>,
    // 旧 schema 字段，向后兼容历史 intent_json
    #[serde(default)]
    reference: Option<String>,
    #[serde(default)]
    reference_person: Option<String>,
    #[serde(default = "default_confidence")]
    confidence: f32,
    #[serde(default)]
    reasoning: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawSubject {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    kind: Option<String>,
}

fn default_confidence() -> f32 {
    0.5
}

fn parse_kind(s: &str) -> Option<QueryKind> {
    match s {
        "exact_title" => Some(QueryKind::ExactTitle),
        "similar_to" => Some(QueryKind::SimilarTo),
        "person" => Some(QueryKind::Person),
        "attribute" => Some(QueryKind::Attribute),
        "descriptive" => Some(QueryKind::Descriptive),
        _ => None,
    }
}

/// 把 raw 解析结果折叠成 Option<Subject>。新 schema 优先，旧字段兜底。
fn build_subject(raw: &RawClassification, query_kind: QueryKind) -> Option<Subject> {
    // 新 shape 优先
    if let Some(rs) = raw.subject.as_ref() {
        let name = rs.name.as_deref().unwrap_or("").trim();
        let kind_str = rs.kind.as_deref().unwrap_or("").trim();
        if !name.is_empty() {
            if let Some(kind) = SubjectKind::parse(kind_str) {
                return Some(Subject {
                    name: name.to_string(),
                    kind,
                });
            }
        }
    }
    // 旧 shape 回退：reference_person 优先，再 reference
    if let Some(p) = raw.reference_person.as_deref() {
        let p = p.trim();
        if !p.is_empty() {
            return Some(Subject {
                name: p.to_string(),
                kind: SubjectKind::Person,
            });
        }
    }
    if let Some(r) = raw.reference.as_deref() {
        let r = r.trim();
        if !r.is_empty() {
            // 旧 schema 中 reference 字段在 exact_title / similar_to 都是电影名
            let kind = match query_kind {
                QueryKind::Person => SubjectKind::Person,
                _ => SubjectKind::Movie,
            };
            return Some(Subject {
                name: r.to_string(),
                kind,
            });
        }
    }
    None
}

/// 把 LLM 返回的原文解析成 QueryClassification。纯字符串处理，便于单测。
pub fn parse_classification(raw: &str) -> QueryClassification {
    let json_str = extract_json(raw);
    let parsed: RawClassification = match serde_json::from_str(&json_str) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("failed to parse classification JSON: {}. raw={}", e, raw);
            return QueryClassification::fallback_descriptive("parse error");
        }
    };
    let kind = match parse_kind(parsed.kind.trim()) {
        Some(k) => k,
        None => {
            tracing::warn!("unknown classification type: {}", parsed.kind);
            return QueryClassification::fallback_descriptive(format!(
                "unknown type: {}",
                parsed.kind
            ));
        }
    };
    let confidence = parsed.confidence.clamp(0.0, 1.0);
    let subject = build_subject(&parsed, kind);
    QueryClassification {
        kind,
        subject,
        confidence,
        reasoning: parsed.reasoning,
    }
    .normalized(DEFAULT_CONFIDENCE_THRESHOLD)
}

// 本地副本，避免模块间循环依赖。与 recommend.rs 中的同名函数行为一致。
fn extract_json(text: &str) -> String {
    let trimmed = text.trim();
    if let Some(start) = trimmed.find('{') {
        if let Some(end) = trimmed.rfind('}') {
            if end >= start {
                return trimmed[start..=end].to_string();
            }
        }
    }
    trimmed.to_string()
}

/// 调用 LLM 做分类。失败一律降级 descriptive，不向上抛错——分类是增强路径，
/// 不能因为它挂掉把整个推荐管线带挂。
pub async fn classify_query(
    llm: &LlmClient,
    pool: &SqlitePool,
    query: &str,
    locale: &str,
) -> QueryClassification {
    let template = match db::get_prompt_override(pool, "query-classify", locale).await {
        Ok(Some(v)) => v,
        _ => default_prompt(locale).to_string(),
    };

    let (total, directors, cast) = match db::get_library_stats(pool).await {
        Ok(stats) => (
            stats.total.to_string(),
            stats
                .directors
                .iter()
                .take(30)
                .map(|(d, _)| d.as_str())
                .collect::<Vec<_>>()
                .join(if locale == "en" { ", " } else { "、" }),
            stats
                .cast
                .iter()
                .take(30)
                .map(|(c, _)| c.as_str())
                .collect::<Vec<_>>()
                .join(if locale == "en" { ", " } else { "、" }),
        ),
        Err(e) => {
            tracing::warn!("get_library_stats failed while classifying: {}", e);
            ("0".to_string(), String::new(), String::new())
        }
    };

    let system_prompt = template
        .replace("{{total}}", &total)
        .replace("{{directors}}", &directors)
        .replace("{{cast}}", &cast)
        .replace("{{query}}", query);

    match llm.chat(&system_prompt, query).await {
        Ok(resp) => parse_classification(&resp),
        Err(e) => {
            tracing::warn!("classifier LLM call failed: {}", e);
            QueryClassification::fallback_descriptive(format!("llm error: {}", e))
        }
    }
}

fn default_prompt(locale: &str) -> &'static str {
    match locale {
        "en" => include_str!("../../prompts/query-classify.en.md"),
        _ => include_str!("../../prompts/query-classify.md"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ===== 新 schema 解析 =====

    #[test]
    fn parse_new_exact_title() {
        let raw = r#"{"type":"exact_title","subject":{"name":"海底总动员","kind":"movie"},"confidence":0.95,"reasoning":"直接电影名"}"#;
        let c = parse_classification(raw);
        assert_eq!(c.kind, QueryKind::ExactTitle);
        let s = c.subject.unwrap();
        assert_eq!(s.name, "海底总动员");
        assert_eq!(s.kind, SubjectKind::Movie);
    }

    #[test]
    fn parse_new_similar_to_movie() {
        let raw = r#"{"type":"similar_to","subject":{"name":"Finding Nemo","kind":"movie"},"confidence":0.9}"#;
        let c = parse_classification(raw);
        assert_eq!(c.kind, QueryKind::SimilarTo);
        let s = c.subject.unwrap();
        assert_eq!(s.name, "Finding Nemo");
        assert_eq!(s.kind, SubjectKind::Movie);
    }

    #[test]
    fn parse_new_similar_to_person() {
        let raw = r#"{"type":"similar_to","subject":{"name":"小津安二郎","kind":"person"},"confidence":0.9}"#;
        let c = parse_classification(raw);
        assert_eq!(c.kind, QueryKind::SimilarTo);
        let s = c.subject.unwrap();
        assert_eq!(s.name, "小津安二郎");
        assert_eq!(s.kind, SubjectKind::Person);
    }

    #[test]
    fn parse_new_similar_to_movement() {
        let raw = r#"{"type":"similar_to","subject":{"name":"法国新浪潮","kind":"movement"},"confidence":0.9}"#;
        let c = parse_classification(raw);
        assert_eq!(c.kind, QueryKind::SimilarTo);
        let s = c.subject.unwrap();
        assert_eq!(s.kind, SubjectKind::Movement);
    }

    #[test]
    fn parse_new_person() {
        let raw = r#"{"type":"person","subject":{"name":"诺兰","kind":"person"},"confidence":0.92}"#;
        let c = parse_classification(raw);
        assert_eq!(c.kind, QueryKind::Person);
        assert_eq!(c.subject.as_ref().unwrap().kind, SubjectKind::Person);
    }

    #[test]
    fn parse_new_attribute_no_subject() {
        let raw = r#"{"type":"attribute","subject":null,"confidence":0.9}"#;
        let c = parse_classification(raw);
        assert_eq!(c.kind, QueryKind::Attribute);
        assert!(c.subject.is_none());
    }

    #[test]
    fn parse_new_descriptive_no_subject() {
        let raw = r#"{"type":"descriptive","confidence":0.85,"reasoning":"氛围"}"#;
        let c = parse_classification(raw);
        assert_eq!(c.kind, QueryKind::Descriptive);
    }

    // ===== 旧 schema 兼容 =====

    #[test]
    fn legacy_exact_title_with_reference() {
        let raw = r#"{"type":"exact_title","reference":"海底总动员","confidence":0.95}"#;
        let c = parse_classification(raw);
        assert_eq!(c.kind, QueryKind::ExactTitle);
        let s = c.subject.unwrap();
        assert_eq!(s.name, "海底总动员");
        assert_eq!(s.kind, SubjectKind::Movie);
    }

    #[test]
    fn legacy_similar_to_with_reference() {
        let raw = r#"{"type":"similar_to","reference":"Finding Nemo","confidence":0.9}"#;
        let c = parse_classification(raw);
        assert_eq!(c.kind, QueryKind::SimilarTo);
        let s = c.subject.unwrap();
        assert_eq!(s.name, "Finding Nemo");
        assert_eq!(s.kind, SubjectKind::Movie);
    }

    #[test]
    fn legacy_person_with_reference_person() {
        let raw = r#"{"type":"person","reference_person":"诺兰","confidence":0.92}"#;
        let c = parse_classification(raw);
        assert_eq!(c.kind, QueryKind::Person);
        let s = c.subject.unwrap();
        assert_eq!(s.name, "诺兰");
        assert_eq!(s.kind, SubjectKind::Person);
    }

    // ===== 降级路径 =====

    #[test]
    fn low_confidence_downgrades_to_descriptive() {
        let raw = r#"{"type":"exact_title","subject":{"name":"x","kind":"movie"},"confidence":0.3}"#;
        let c = parse_classification(raw);
        assert_eq!(c.kind, QueryKind::Descriptive);
        assert!(c.reasoning.as_deref().unwrap().contains("below threshold"));
    }

    #[test]
    fn missing_subject_for_exact_title_downgrades() {
        let raw = r#"{"type":"exact_title","confidence":0.9}"#;
        let c = parse_classification(raw);
        assert_eq!(c.kind, QueryKind::Descriptive);
        assert!(c.reasoning.as_deref().unwrap().contains("missing subject"));
    }

    #[test]
    fn missing_subject_for_person_downgrades() {
        let raw = r#"{"type":"person","confidence":0.9}"#;
        let c = parse_classification(raw);
        assert_eq!(c.kind, QueryKind::Descriptive);
    }

    #[test]
    fn missing_subject_for_similar_to_downgrades() {
        let raw = r#"{"type":"similar_to","confidence":0.9}"#;
        let c = parse_classification(raw);
        assert_eq!(c.kind, QueryKind::Descriptive);
    }

    #[test]
    fn exact_title_with_non_movie_subject_downgrades() {
        let raw = r#"{"type":"exact_title","subject":{"name":"X","kind":"person"},"confidence":0.9}"#;
        let c = parse_classification(raw);
        assert_eq!(c.kind, QueryKind::Descriptive);
        assert!(c.reasoning.as_deref().unwrap().contains("subject.kind=movie"));
    }

    #[test]
    fn person_with_non_person_subject_downgrades() {
        let raw = r#"{"type":"person","subject":{"name":"X","kind":"movie"},"confidence":0.9}"#;
        let c = parse_classification(raw);
        assert_eq!(c.kind, QueryKind::Descriptive);
        assert!(c
            .reasoning
            .as_deref()
            .unwrap()
            .contains("subject.kind=person"));
    }

    #[test]
    fn unknown_subject_kind_falls_through_to_legacy() {
        // subject.kind 不合法 → build_subject 跳过新 shape；旧字段也没有 → subject = None
        let raw = r#"{"type":"similar_to","subject":{"name":"X","kind":"alien"},"confidence":0.9}"#;
        let c = parse_classification(raw);
        assert_eq!(c.kind, QueryKind::Descriptive);
    }

    #[test]
    fn empty_subject_name_downgrades() {
        let raw = r#"{"type":"similar_to","subject":{"name":"","kind":"movie"},"confidence":0.9}"#;
        let c = parse_classification(raw);
        assert_eq!(c.kind, QueryKind::Descriptive);
    }

    #[test]
    fn unknown_type_downgrades() {
        let raw = r#"{"type":"mystery","confidence":0.9}"#;
        let c = parse_classification(raw);
        assert_eq!(c.kind, QueryKind::Descriptive);
    }

    #[test]
    fn parse_strips_markdown_fence() {
        let raw = r#"```json
{"type":"exact_title","subject":{"name":"千与千寻","kind":"movie"},"confidence":0.95}
```"#;
        let c = parse_classification(raw);
        assert_eq!(c.kind, QueryKind::ExactTitle);
        assert_eq!(c.subject.unwrap().name, "千与千寻");
    }

    #[test]
    fn parse_malformed_is_descriptive() {
        let c = parse_classification("this is not JSON at all");
        assert_eq!(c.kind, QueryKind::Descriptive);
    }

    #[test]
    fn confidence_clamped_to_0_1() {
        let raw = r#"{"type":"exact_title","subject":{"name":"a","kind":"movie"},"confidence":2.5}"#;
        let c = parse_classification(raw);
        assert_eq!(c.kind, QueryKind::ExactTitle);
        assert!((c.confidence - 1.0).abs() < 1e-5);

        let raw2 = r#"{"type":"exact_title","subject":{"name":"a","kind":"movie"},"confidence":-1.0}"#;
        let c2 = parse_classification(raw2);
        assert_eq!(c2.kind, QueryKind::Descriptive);
    }

    // ===== 新旧并存：新 shape 优先 =====

    #[test]
    fn new_subject_wins_over_legacy_fields() {
        let raw = r#"{"type":"similar_to","subject":{"name":"NEW","kind":"person"},"reference":"OLD","reference_person":"OLDPERSON","confidence":0.9}"#;
        let c = parse_classification(raw);
        let s = c.subject.unwrap();
        assert_eq!(s.name, "NEW");
        assert_eq!(s.kind, SubjectKind::Person);
    }
}
