use serde::Serialize;

/// One parsed row from a DouBanExport CSV. Fields the importer cares about.
/// The CSV always has the same columns; everything we don't use (poster, comment,
/// rate timestamp) is dropped during parse.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct DoubanRecord {
    pub douban_subject_id: String,
    pub raw_title: String,
    pub parsed_title_zh: Option<String>,
    pub parsed_title_en: Option<String>,
    pub year: Option<i64>,
    pub country: Option<String>,
    pub douban_url: String,
}

#[derive(Debug)]
pub enum ParseError {
    EmptyHeader,
    MalformedHeader(String),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyHeader => write!(f, "CSV header is empty"),
            Self::MalformedHeader(s) => {
                write!(f, "CSV header doesn't look like a DouBanExport export: {}", s)
            }
        }
    }
}

impl std::error::Error for ParseError {}

const EXPECTED_HEADER_COLS: usize = 8;

/// Parse a DouBanExport CSV. Lines that fail to yield 8 fields or that don't
/// have a usable subject id are silently skipped — the importer's caller treats
/// "fewer rows than expected" as the user's problem to inspect, not a hard
/// failure that aborts the whole upload.
pub fn parse_csv(content: &str) -> Result<Vec<DoubanRecord>, ParseError> {
    let stripped = strip_bom(content);
    let rows = read_csv_rows(stripped);

    let mut iter = rows.into_iter();
    let header = iter.next().ok_or(ParseError::EmptyHeader)?;
    if header.len() < EXPECTED_HEADER_COLS {
        return Err(ParseError::MalformedHeader(header.join(",")));
    }
    // Sanity-check the first column is "封面" (cover); a typo or off-by-one
    // export shouldn't silently parse as data.
    if !header[0].contains("封面") {
        return Err(ParseError::MalformedHeader(header.join(",")));
    }

    let mut records = Vec::new();
    for fields in iter {
        if fields.iter().all(|f| f.trim().is_empty()) {
            continue;
        }
        if fields.len() < EXPECTED_HEADER_COLS {
            continue;
        }
        let raw_title = fields[1].trim().to_string();
        let release_date = fields[5].trim();
        let country = fields[6].trim().to_string();
        let douban_url = fields[7].trim().to_string();

        let Some(douban_subject_id) = extract_subject_id(&douban_url) else {
            continue;
        };
        if raw_title.is_empty() {
            continue;
        }

        let (parsed_title_zh, parsed_title_en) = split_title(&raw_title);
        let year = release_date.get(0..4).and_then(|y| y.parse::<i64>().ok());

        records.push(DoubanRecord {
            douban_subject_id,
            raw_title,
            parsed_title_zh,
            parsed_title_en,
            year,
            country: if country.is_empty() { None } else { Some(country) },
            douban_url,
        });
    }

    Ok(records)
}

fn strip_bom(s: &str) -> &str {
    s.strip_prefix('\u{feff}').unwrap_or(s)
}

/// Minimal CSV reader: fields are double-quoted, commas separate, doubled quotes
/// (`""`) inside a quoted field encode a literal `"`. Newlines outside quoted
/// fields end a record. This is enough for DouBanExport's output and avoids
/// pulling in the `csv` crate.
fn read_csv_rows(input: &str) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    let mut current_row: Vec<String> = Vec::new();
    let mut current_field = String::new();
    let mut in_quotes = false;
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        if in_quotes {
            match c {
                '"' => {
                    if chars.peek() == Some(&'"') {
                        chars.next();
                        current_field.push('"');
                    } else {
                        in_quotes = false;
                    }
                }
                _ => current_field.push(c),
            }
        } else {
            match c {
                '"' => in_quotes = true,
                ',' => {
                    current_row.push(std::mem::take(&mut current_field));
                }
                '\r' => {
                    // swallow; \n closes the record
                }
                '\n' => {
                    current_row.push(std::mem::take(&mut current_field));
                    rows.push(std::mem::take(&mut current_row));
                }
                _ => current_field.push(c),
            }
        }
    }
    // Trailing line without newline.
    if !current_field.is_empty() || !current_row.is_empty() {
        current_row.push(current_field);
        rows.push(current_row);
    }
    rows
}

fn extract_subject_id(url: &str) -> Option<String> {
    // matches /subject/<digits>/ in any URL shape
    let after = url.split("/subject/").nth(1)?;
    let id: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
    if id.is_empty() {
        None
    } else {
        Some(id)
    }
}

/// Split a multilingual title field like
/// `中文名/EnglishName/港译/台译` into (zh, en).
///
/// Rules (keep simple, fall back rather than getting clever):
/// - Split on `/`
/// - First segment containing a CJK character → zh
/// - First segment that is mostly ASCII letters → en
/// - Skip segments wrapped in `(港)`/`(台)` annotations or that contain only
///   parentheses noise (those are alt translations of zh, not en)
pub fn split_title(raw: &str) -> (Option<String>, Option<String>) {
    let segments: Vec<&str> = raw.split('/').map(str::trim).filter(|s| !s.is_empty()).collect();

    let mut zh: Option<String> = None;
    let mut en: Option<String> = None;

    for seg in segments {
        let stripped_annot = strip_region_annotation(seg);
        if zh.is_none() && has_cjk(stripped_annot) {
            zh = Some(stripped_annot.to_string());
            continue;
        }
        if en.is_none() && is_ascii_titleish(stripped_annot) {
            en = Some(stripped_annot.to_string());
            continue;
        }
    }

    (zh, en)
}

fn has_cjk(s: &str) -> bool {
    s.chars().any(|c| {
        let cp = c as u32;
        (0x4E00..=0x9FFF).contains(&cp)
            || (0x3400..=0x4DBF).contains(&cp)
            || (0x3040..=0x30FF).contains(&cp) // Japanese kana, sometimes appears
    })
}

/// Treat a string as an English/Latin title if it contains at least one ASCII
/// letter and no CJK characters. We allow digits, punctuation and spaces — many
/// English titles have those.
fn is_ascii_titleish(s: &str) -> bool {
    if has_cjk(s) {
        return false;
    }
    s.chars().any(|c| c.is_ascii_alphabetic())
}

/// Strip trailing `(港)` / `(台)` / `(港译)` etc. annotations that豆瓣 attaches to
/// regional alt titles. These should not pollute the comparison.
fn strip_region_annotation(s: &str) -> &str {
    // Walk back from the end and trim a trailing parenthesized group.
    let trimmed = s.trim_end();
    if let Some(idx) = trimmed.rfind('(') {
        if trimmed.ends_with(')') {
            let inside = &trimmed[idx + 1..trimmed.len() - 1];
            if inside.chars().count() <= 4 {
                // Short annotation like 港 / 台 / 港译 — strip it.
                return trimmed[..idx].trim_end();
            }
        }
    }
    trimmed
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "\u{feff}封面,标题,个人评分,打分日期,我的短评,上映日期,制片国家,条目链接\n\
\"https://img/p1.jpg\",\"通天塔/Babel/巴别塔(港)/火线交错(台)\",\"\",\"2007/03/01\",\"\",\"2006/11/10\",\"美国\",\"https://movie.douban.com/subject/1498818/\",\n\
\"https://img/p2.jpg\",\"心慌方/Cube/异次元杀阵/立方体\",\"5\",\"2007/03/01\",\"\",\"1997/09/09\",\"多伦多电影节\",\"https://movie.douban.com/subject/1305903/\",\n";

    #[test]
    fn parses_basic_rows() {
        let rows = parse_csv(SAMPLE).unwrap();
        assert_eq!(rows.len(), 2);

        let r = &rows[0];
        assert_eq!(r.douban_subject_id, "1498818");
        assert_eq!(r.raw_title, "通天塔/Babel/巴别塔(港)/火线交错(台)");
        assert_eq!(r.parsed_title_zh.as_deref(), Some("通天塔"));
        assert_eq!(r.parsed_title_en.as_deref(), Some("Babel"));
        assert_eq!(r.year, Some(2006));
        assert_eq!(r.country.as_deref(), Some("美国"));
        assert_eq!(r.douban_url, "https://movie.douban.com/subject/1498818/");
    }

    #[test]
    fn skips_rows_without_subject_id() {
        let csv = "\u{feff}封面,标题,个人评分,打分日期,我的短评,上映日期,制片国家,条目链接\n\
\"\",\"无链接\",\"\",\"\",\"\",\"\",\"\",\"\"\n";
        let rows = parse_csv(csv).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn empty_lines_are_skipped() {
        let csv = "\u{feff}封面,标题,个人评分,打分日期,我的短评,上映日期,制片国家,条目链接\n\
\n\
\"\",\"通天塔/Babel\",\"\",\"\",\"\",\"2006/11/10\",\"美国\",\"https://movie.douban.com/subject/1498818/\"\n";
        let rows = parse_csv(csv).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn rejects_non_douban_csv_header() {
        let csv = "Name,Year\n\"X\",\"2020\"\n";
        assert!(parse_csv(csv).is_err());
    }

    #[test]
    fn split_title_picks_first_zh_and_en() {
        let (zh, en) = split_title("通天塔/Babel/巴别塔(港)/火线交错(台)");
        assert_eq!(zh.as_deref(), Some("通天塔"));
        assert_eq!(en.as_deref(), Some("Babel"));
    }

    #[test]
    fn split_title_handles_zh_only() {
        let (zh, en) = split_title("疯狂的石头");
        assert_eq!(zh.as_deref(), Some("疯狂的石头"));
        assert_eq!(en, None);
    }

    #[test]
    fn split_title_handles_en_only() {
        let (zh, en) = split_title("Inception");
        assert_eq!(zh, None);
        assert_eq!(en.as_deref(), Some("Inception"));
    }

    #[test]
    fn split_title_strips_region_annotation_when_chosen() {
        // First segment is annotated like "片名(港)" — the annotation gets stripped.
        let (zh, _) = split_title("巴别塔(港)/Babel");
        assert_eq!(zh.as_deref(), Some("巴别塔"));
    }

    #[test]
    fn split_title_skips_alternate_zh_for_en_pick() {
        // Both 通天塔 and 巴别塔 are CJK; we keep first as zh and don't
        // confuse the second one for English.
        let (zh, en) = split_title("通天塔/巴别塔");
        assert_eq!(zh.as_deref(), Some("通天塔"));
        assert_eq!(en, None);
    }

    #[test]
    fn extract_subject_id_basic() {
        assert_eq!(
            extract_subject_id("https://movie.douban.com/subject/1498818/"),
            Some("1498818".to_string())
        );
        assert_eq!(
            extract_subject_id("https://movie.douban.com/subject/1498818/?foo=bar"),
            Some("1498818".to_string())
        );
        assert_eq!(extract_subject_id("https://example.com"), None);
    }

    #[test]
    fn quoted_commas_inside_field() {
        let csv = "\u{feff}封面,标题,个人评分,打分日期,我的短评,上映日期,制片国家,条目链接\n\
\"https://img/p1.jpg\",\"片,有,逗号/Title,With,Commas\",\"\",\"\",\"哈,哈\",\"2020/01/01\",\"美国\",\"https://movie.douban.com/subject/9999/\"\n";
        let rows = parse_csv(csv).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].douban_subject_id, "9999");
        assert_eq!(rows[0].raw_title, "片,有,逗号/Title,With,Commas");
    }
}
