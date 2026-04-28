//! Sidecar evidence collection: scan a movie directory for "inner" hints
//! beyond the parent dir name — file stems, child dir names, Blu-ray BDMV
//! META XML disc title. All become extra (title, year) candidates fed to
//! TMDB search alongside the parent dir name.
//!
//! See docs/specs/2026-04-26-sidecar-evidence-design.md.
//!
//! Design notes:
//! - Only one level deep; we don't recurse into nested BDMV/etc.
//! - Hard-coded skip list for known noise dir names.
//! - File extension whitelist: only types likely to carry meaningful naming.
//! - quick-xml is used permissively (no namespace validation) — Blu-ray
//!   META XML structure is uniform across discs in practice.

use std::path::Path;

use quick_xml::events::Event;
use quick_xml::Reader;
use tokio::fs;

use crate::scanner::parser::{parse_directory_name, ParsedName};

/// Directory names that hold disc structure / extras / noise — skip whole-cloth
/// when collecting child dir candidates. Lowercase compare.
const NOISE_DIR_NAMES: &[&str] = &[
    "bdmv",
    "certificate",
    "aacs",
    "meta",
    "extras",
    "extra",
    "samples",
    "sample",
    "bonus",
    "trailers",
    "trailer",
    "特典",
    "花絮",
    "scrapbook",
    "scraps",
    "video_ts",
    "audio_ts",
];

/// File extensions worth taking the stem from. Lowercase compare.
const TITLE_BEARING_EXTS: &[&str] = &[
    "mkv", "mp4", "iso", "m2ts", "ts", "avi", "mov", "wmv", "m4v", "nfo",
];

#[derive(Debug, Default, Clone)]
pub struct SidecarEvidence {
    pub candidates: Vec<ParsedName>,
}

/// Collect inner-name evidence for a movie directory.
///
/// Returns an empty result on any IO error — caller falls back to parent dir
/// candidate alone. Sidecar evidence is best-effort, never a hard dependency.
pub async fn collect_evidence(dir_path: &Path) -> SidecarEvidence {
    let mut raw_candidates: Vec<ParsedName> = Vec::new();

    let mut entries = match fs::read_dir(dir_path).await {
        Ok(e) => e,
        Err(_) => return SidecarEvidence::default(),
    };

    let mut bdmv_meta_dir: Option<std::path::PathBuf> = None;

    loop {
        let entry = match entries.next_entry().await {
            Ok(Some(e)) => e,
            Ok(None) => break,
            Err(_) => continue,
        };

        let file_type = match entry.file_type().await {
            Ok(t) => t,
            Err(_) => continue,
        };
        let name = match entry.file_name().into_string() {
            Ok(n) => n,
            Err(_) => continue,
        };

        if file_type.is_dir() {
            let lower = name.to_lowercase();
            if lower.eq_ignore_ascii_case("bdmv") {
                bdmv_meta_dir = Some(entry.path().join("META").join("DL"));
                continue;
            }
            if NOISE_DIR_NAMES.iter().any(|n| n.eq_ignore_ascii_case(&lower)) {
                continue;
            }
            raw_candidates.push(parse_directory_name(&name));
        } else if file_type.is_file() {
            let stem_and_ext = split_stem_ext(&name);
            if let Some((stem, ext)) = stem_and_ext {
                if TITLE_BEARING_EXTS
                    .iter()
                    .any(|e| e.eq_ignore_ascii_case(ext))
                {
                    raw_candidates.push(parse_directory_name(stem));
                }
            }
        }
    }

    if let Some(meta_dir) = bdmv_meta_dir {
        if let Some(disc_title) = read_bdmv_meta_disc_title(&meta_dir).await {
            raw_candidates.push(parse_directory_name(&disc_title));
        }
    }

    SidecarEvidence {
        candidates: dedupe(raw_candidates),
    }
}

/// Split "Foo.Bar.2020.mkv" into ("Foo.Bar.2020", "mkv"). Returns None when
/// there's no extension or it's empty.
fn split_stem_ext(name: &str) -> Option<(&str, &str)> {
    let dot = name.rfind('.')?;
    if dot == 0 || dot == name.len() - 1 {
        return None;
    }
    Some((&name[..dot], &name[dot + 1..]))
}

/// Drop ParsedName entries that are duplicates or have empty titles.
fn dedupe(mut items: Vec<ParsedName>) -> Vec<ParsedName> {
    items.retain(|p| !p.title.trim().is_empty());
    let mut seen: Vec<(String, Option<u16>)> = Vec::new();
    items.retain(|p| {
        let key = (p.title.to_lowercase(), p.year);
        if seen.iter().any(|s| *s == key) {
            false
        } else {
            seen.push(key);
            true
        }
    });
    items
}

/// Look in `<dir>/BDMV/META/DL/` for `bdmt_eng.xml` first, falling back to any
/// `bdmt_*.xml` that parses successfully. Returns the disc title if found.
async fn read_bdmv_meta_disc_title(meta_dir: &Path) -> Option<String> {
    let preferred = meta_dir.join("bdmt_eng.xml");
    if let Ok(xml) = fs::read_to_string(&preferred).await {
        if let Some(title) = parse_bdmv_meta(&xml) {
            return Some(title);
        }
    }

    let mut entries = fs::read_dir(meta_dir).await.ok()?;
    while let Some(entry) = entries.next_entry().await.ok().flatten() {
        let name = entry.file_name().into_string().ok()?;
        let lower = name.to_lowercase();
        if !lower.starts_with("bdmt_") || !lower.ends_with(".xml") {
            continue;
        }
        if let Ok(xml) = fs::read_to_string(entry.path()).await {
            if let Some(title) = parse_bdmv_meta(&xml) {
                return Some(title);
            }
        }
    }
    None
}

/// Extract the Blu-ray disc title from a `bdmt_*.xml` document.
///
/// Standard structure:
///
/// ```xml
/// <disclib xmlns="urn:BDA:bdmv;disclib">
///   <di:discinfo xmlns:di="urn:BDA:bdmv;discinfo">
///     <di:title>
///       <di:name>Gravity</di:name>
///     </di:title>
///   </di:discinfo>
/// </disclib>
/// ```
///
/// We don't validate namespaces — we just look for the first text inside a
/// `name` element nested under `title`. This is robust enough for the BDA
/// standard structure and tolerates minor authoring variations.
pub fn parse_bdmv_meta(xml: &str) -> Option<String> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();

    let mut in_title = false;
    let mut in_name = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let tag = local_name(e.name().as_ref());
                if tag.eq_ignore_ascii_case("title") {
                    in_title = true;
                } else if in_title && tag.eq_ignore_ascii_case("name") {
                    in_name = true;
                }
            }
            Ok(Event::End(e)) => {
                let tag = local_name(e.name().as_ref());
                if tag.eq_ignore_ascii_case("name") {
                    in_name = false;
                } else if tag.eq_ignore_ascii_case("title") {
                    in_title = false;
                }
            }
            Ok(Event::Text(e)) => {
                if in_title && in_name {
                    if let Ok(text) = e.unescape() {
                        let trimmed = text.trim();
                        if !trimmed.is_empty() {
                            return Some(trimmed.to_string());
                        }
                    }
                }
            }
            Ok(Event::Eof) => return None,
            Err(_) => return None,
            _ => {}
        }
        buf.clear();
    }
}

/// Strip XML namespace prefix: `di:name` → `name`, `name` → `name`.
fn local_name(qname: &[u8]) -> String {
    let s = std::str::from_utf8(qname).unwrap_or("");
    match s.rfind(':') {
        Some(i) => s[i + 1..].to_string(),
        None => s.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write_file(root: &Path, rel: &str, content: &str) {
        let path = root.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }

    fn make_dir(root: &Path, rel: &str) {
        std::fs::create_dir_all(root.join(rel)).unwrap();
    }

    #[test]
    fn parse_bdmv_meta_extracts_disc_title() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<disclib xmlns="urn:BDA:bdmv;disclib">
  <di:discinfo xmlns:di="urn:BDA:bdmv;discinfo">
    <di:title>
      <di:name>Gravity</di:name>
    </di:title>
    <di:language>eng</di:language>
  </di:discinfo>
</disclib>"#;
        assert_eq!(parse_bdmv_meta(xml), Some("Gravity".to_string()));
    }

    #[test]
    fn parse_bdmv_meta_handles_missing_namespace_prefix() {
        let xml = r#"<disclib>
  <discinfo>
    <title><name>The Bridge</name></title>
  </discinfo>
</disclib>"#;
        assert_eq!(parse_bdmv_meta(xml), Some("The Bridge".to_string()));
    }

    #[test]
    fn parse_bdmv_meta_returns_none_for_empty_or_missing_name() {
        assert_eq!(parse_bdmv_meta(""), None);
        let xml_empty = r#"<disclib><discinfo><title><name></name></title></discinfo></disclib>"#;
        assert_eq!(parse_bdmv_meta(xml_empty), None);
        let xml_no_title = r#"<disclib><discinfo></discinfo></disclib>"#;
        assert_eq!(parse_bdmv_meta(xml_no_title), None);
    }

    #[test]
    fn parse_bdmv_meta_does_not_match_name_outside_title() {
        // <name> elsewhere should NOT be picked up.
        let xml = r#"<disclib>
  <discinfo>
    <thumbnail name="ignore"/>
    <description><name>Description Name (NOT title)</name></description>
  </discinfo>
</disclib>"#;
        assert_eq!(parse_bdmv_meta(xml), None);
    }

    #[test]
    fn parse_bdmv_meta_robust_to_malformed_xml() {
        // The contract is: no panic on malformed input. Returning a partial
        // title from a truncated document is acceptable; what we forbid is
        // crashing or hanging.
        let _ = parse_bdmv_meta(r#"<disclib><discinfo><title><name>Half"#);
        let _ = parse_bdmv_meta(r#"<<<not xml at all>>>"#);
        let _ = parse_bdmv_meta("\u{0}\u{1}\u{2}");
    }

    #[tokio::test]
    async fn collect_evidence_skips_noise_dirs() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("Movie");
        std::fs::create_dir_all(&dir).unwrap();
        // Noise dirs: should be ignored
        make_dir(&dir, "BDMV");
        make_dir(&dir, "CERTIFICATE");
        make_dir(&dir, "extras");
        // Real candidate dir: should be parsed
        make_dir(&dir, "The.Real.Title.2020.1080p.BluRay-GROUP");

        let ev = collect_evidence(&dir).await;
        assert_eq!(ev.candidates.len(), 1);
        assert_eq!(ev.candidates[0].title, "The Real Title");
        assert_eq!(ev.candidates[0].year, Some(2020));
    }

    #[tokio::test]
    async fn collect_evidence_picks_up_file_stems() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("[集结号].原盘DIY");
        std::fs::create_dir_all(&dir).unwrap();
        write_file(&dir, "Assembly 2007.nfo", "junk");
        write_file(&dir, "movie.mkv", "junk"); // generic name → just "movie"

        let ev = collect_evidence(&dir).await;
        let titles: Vec<_> = ev.candidates.iter().map(|p| p.title.as_str()).collect();
        assert!(titles.contains(&"Assembly"), "got {:?}", titles);
        let assembly = ev
            .candidates
            .iter()
            .find(|p| p.title == "Assembly")
            .unwrap();
        assert_eq!(assembly.year, Some(2007));
    }

    #[tokio::test]
    async fn collect_evidence_ignores_unknown_extensions() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("Movie");
        std::fs::create_dir_all(&dir).unwrap();
        write_file(&dir, "thumb.jpg", "x"); // not in TITLE_BEARING_EXTS
        write_file(&dir, "data.json", "x");
        write_file(&dir, "Real Title 2019.mkv", "x");

        let ev = collect_evidence(&dir).await;
        assert_eq!(ev.candidates.len(), 1);
        assert_eq!(ev.candidates[0].title, "Real Title");
    }

    #[tokio::test]
    async fn collect_evidence_reads_bdmv_meta_disc_title() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("GRAVITY_HDCLUB");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::create_dir_all(dir.join("BDMV").join("META").join("DL")).unwrap();
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<disclib xmlns="urn:BDA:bdmv;disclib">
  <di:discinfo xmlns:di="urn:BDA:bdmv;discinfo">
    <di:title><di:name>Gravity</di:name></di:title>
  </di:discinfo>
</disclib>"#;
        write_file(&dir, "BDMV/META/DL/bdmt_eng.xml", xml);
        // CERTIFICATE noise — should be skipped
        make_dir(&dir, "CERTIFICATE");

        let ev = collect_evidence(&dir).await;
        let titles: Vec<_> = ev.candidates.iter().map(|p| p.title.as_str()).collect();
        assert!(titles.contains(&"Gravity"), "got {:?}", titles);
    }

    #[tokio::test]
    async fn collect_evidence_falls_back_to_non_eng_bdmt_xml() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("FOO");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::create_dir_all(dir.join("BDMV").join("META").join("DL")).unwrap();
        let xml = r#"<disclib><discinfo><title><name>外语标题</name></title></discinfo></disclib>"#;
        write_file(&dir, "BDMV/META/DL/bdmt_jpn.xml", xml);

        let ev = collect_evidence(&dir).await;
        let titles: Vec<_> = ev.candidates.iter().map(|p| p.title.as_str()).collect();
        assert!(titles.contains(&"外语标题"), "got {:?}", titles);
    }

    #[tokio::test]
    async fn collect_evidence_dedupes() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("X");
        std::fs::create_dir_all(&dir).unwrap();
        // Same parsed (title, year) appearing in both a child dir and a file stem
        make_dir(&dir, "Inception 2010");
        write_file(&dir, "Inception 2010.mkv", "x");

        let ev = collect_evidence(&dir).await;
        let inception_count = ev
            .candidates
            .iter()
            .filter(|p| p.title == "Inception" && p.year == Some(2010))
            .count();
        assert_eq!(inception_count, 1, "duplicates should be removed");
    }

    #[tokio::test]
    async fn collect_evidence_returns_empty_on_missing_dir() {
        let tmp = TempDir::new().unwrap();
        let nonexistent = tmp.path().join("does-not-exist");
        let ev = collect_evidence(&nonexistent).await;
        assert!(ev.candidates.is_empty());
    }

    #[tokio::test]
    async fn collect_evidence_drops_empty_titles() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("X");
        std::fs::create_dir_all(&dir).unwrap();
        // File with name that parses to empty title (only tech tokens)
        write_file(&dir, "1080p.BluRay.x264.mkv", "x");
        let ev = collect_evidence(&dir).await;
        // Either no candidate or non-empty title — must not have empty-title row
        assert!(ev.candidates.iter().all(|p| !p.title.trim().is_empty()));
    }
}
