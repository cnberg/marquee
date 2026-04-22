//! Movie directory/filename parser — Rust port of guessit's core logic.
//!
//! Strategy:
//! 1. Normalize separators (dots, underscores → spaces)
//! 2. Extract year (4-digit 1900–2099)
//! 3. Strip all recognized technical tokens (source, codec, resolution, audio, etc.)
//! 4. Strip release group (dash-separated suffix after known tokens)
//! 5. Strip container extensions
//! 6. Strip bracketed content (subtitle groups, tags)
//! 7. What remains is the title

use once_cell::sync::Lazy;
use regex::Regex;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedName {
    pub title: String,
    /// Alternative title (e.g. Chinese title when main title is English, or vice versa)
    pub alt_title: Option<String>,
    pub year: Option<u16>,
}

/// Regex to detect Chinese characters
static CHINESE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"[\x{4e00}-\x{9fff}\x{3400}-\x{4dbf}]+").unwrap()
});

/// Split a title string into Chinese and non-Chinese parts.
/// Returns (chinese_part, english_part), either may be empty.
fn split_cn_en(text: &str) -> (String, String) {
    let mut cn_parts: Vec<String> = Vec::new();
    let mut en_parts: Vec<String> = Vec::new();

    // Walk through the text and separate Chinese vs non-Chinese segments
    let mut last_end = 0;
    for m in CHINESE_RE.find_iter(text) {
        let before = text[last_end..m.start()].trim();
        if !before.is_empty() {
            en_parts.push(before.to_string());
        }
        cn_parts.push(m.as_str().to_string());
        last_end = m.end();
    }
    let after = text[last_end..].trim();
    if !after.is_empty() {
        en_parts.push(after.to_string());
    }

    let cn = cn_parts.join("").trim().to_string();
    let en = en_parts.join(" ").trim().to_string();
    (cn, en)
}

/// Clean up a title candidate: remove noise fragments like "DIY简繁中字", "国英双语", etc.
fn clean_cn_noise(text: &str) -> String {
    let noise = Regex::new(r"(?i)(?:DIY)?(?:简繁)?中[字英]?字?幕?|国英双语|[简繁]中字|中英字幕|￡\S+").unwrap();
    let result = noise.replace_all(text, " ");
    MULTI_SPACE_RE.replace_all(result.trim(), " ").trim().to_string()
}

// ---------------------------------------------------------------------------
// All known technical tokens, grouped by category.
// These are case-insensitive word-boundary patterns derived from guessit.
// ---------------------------------------------------------------------------

/// Video sources / release types
const SOURCES: &[&str] = &[
    // Blu-ray variants
    "Blu-?ray", "BluRay", "BDRip", "BRRip", "BD", "BD[259]", "BD25", "BD50",
    "BDMV", "BDREMUX", "Blu-?ray-?Remux",
    // UHD Blu-ray
    "Ultra-?Blu-?ray", "UHD-?BluRay", "UHD-?Blu-?ray",
    // DVD variants
    "DVD", "DVDRip", "DVDR", "DVD-?R", "DVD-?[59]", "VIDEO-?TS",
    "DVDScr", "DVDScreener",
    // HD-DVD
    "HD-?DVD", "HD-?DVDRip",
    // Web
    "WEB-?DL", "WEBRip", "WEB-?Cap", "WEB-?UHD", "WEB",
    "DL-?WEB", "DL-?Mux", "WEB-?Mux",
    // TV
    "HDTV", "HD-?TV", "PDTV", "PD-?TV", "SDTV", "SD-?TV",
    "TVRip", "TV-?Rip", "DSR", "DTH",
    "AHDTV", "UHD-?TV", "UHDRip",
    // Camera / Screener
    "CAM", "CAMRip", "HD-?CAM", "TS", "TELESYNC", "HD-?TS",
    "TC", "TELECINE", "HD-?TC",
    "SCR", "SCREENER", "DVDScr",
    "R5", "R6",
    // Satellite
    "SAT", "SATRip", "DSR", "DSRip",
    // Pay-per-view
    "PPV", "PPVRip",
    // VHS
    "VHS", "VHSRip",
    // LaserDisc
    "LD", "LDRip", "LaserDisc",
    // Workprint
    "WORKPRINT", "WP",
    // Misc source tokens
    "REMUX", "PROPER", "REPACK", "Rerip", "REAL",
    // Digital master
    "DM", "DigitalMaster",
    // VOD
    "VOD", "VODRip",
];

/// Video codecs
const VIDEO_CODECS: &[&str] = &[
    "[hHxX]-?264", "[hHxX]-?265", "HEVC", "AVC(?:HD)?",
    "MPEG-?2", "MPEG-?4", "[hHxX]-?262", "[hHxX]-?263",
    "XviD", "DivX", "DVDivX",
    "VP[789]", "VP80",
    "VC-?1",
    "Rv\\d{2}", // RealVideo
    "HEVC10", "10bit", "8bit", "12bit",
    "Hi10P?", "Hi422P", "Hi444PP",
    "HDR", "HDR10", "HDR10\\+?",
    "Dolby-?Vision", "DV", "SDR",
    "BT-?2020", "HLG",
    "YUV420P10",
];

/// Screen size / resolution
const SCREEN_SIZES: &[&str] = &[
    "\\d{3,4}x\\d{3,4}", // e.g. 1920x1080
    "(?:2160|1440|1080|720|576|540|480|360|4320)[pi]",
    "4[Kk]", "2[Kk]", "8[Kk]",
    "UHD", "FHD", "QHD",
    "(?:Full-?)?HD",
];

/// Audio codecs and channels
const AUDIO: &[&str] = &[
    // Codecs
    "DTS-?HD", "DTS-?X", "DTS-?MA", "DTS-?ES", "DTS",
    "TrueHD", "True-?HD", "Dolby-?TrueHD",
    "Dolby-?Digital-?Plus", "Dolby-?Digital", "Dolby-?Atmos", "Dolby",
    "DD-?EX", "DDP?", "DD\\+?", "DD", "AC-?3D?", "E-?AC-?3",
    "AAC", "AAC2?\\.0", "AAC5?\\.1",
    "FLAC", "LAME", "MP[23]", "OGG", "Vorbis", "Opus",
    "PCM", "LPCM",
    "Atmos",
    // Channels
    "[127]\\.[01](?:ch)?",
    "[2568]ch", "mono", "stereo",
    "(?:5|6|7|8)[\\._][012]",
    // Audio profile
    "MA", "HRA?", "ES",
];

/// Container / file extensions
const CONTAINERS: &[&str] = &[
    "mkv", "avi", "mp4", "m4v", "mov", "wmv", "flv", "webm",
    "mpg", "mpeg", "vob", "ts", "m2ts", "mts",
    "divx", "ogm", "ogv", "3gp", "3g2",
    "iso", "img", "nrg", "bin", "cue",
    "srt", "sub", "idx", "ssa", "ass",
    "nfo", "torrent", "txt",
    "wav", "wma", "ra", "ram",
];

/// Edition / release tags
const EDITIONS: &[&str] = &[
    "Director'?s?[\\._-]?Cut", "DC",
    "Extended[\\._-]?(?:Cut|Version|Edition)?",
    "Theatrical[\\._-]?(?:Cut|Edition)?",
    "Unrated", "Uncut", "Uncensored",
    "Remaster(?:ed)?", "(?:4[Kk][\\._-]?)?Remaster(?:ed)?",
    "Restore(?:d)?", "(?:4[Kk][\\._-]?)?Restore(?:d)?",
    "Limited[\\._-]?(?:Edition)?",
    "Special[\\._-]?Edition",
    "Collector'?s?[\\._-]?Edition",
    "Criterion[\\._-]?(?:Collection|Edition)?",
    "Deluxe[\\._-]?Edition",
    "Anniversary[\\._-]?Edition",
    "IMAX[\\._-]?(?:Edition)?",
    "Ultimate[\\._-]?(?:Edition|Collector'?s?[\\._-]?Edition)?",
    "Fan[\\._-]?Edit(?:ion)?",
    "CC", // Criterion Collection
];

/// Other noise tokens (quality markers, misc tags)
const OTHER: &[&str] = &[
    // Quality / fix
    "PROPER", "REPACK", "Rerip", "REAL",
    "Fix(?:ed)?", "Dirfix", "Nfofix", "Prooffix", "Sample-?Fix",
    "Audio-?Fix(?:ed)?", "Sync-?Fix(?:ed)?",
    // Rip markers
    "Rip", "Re-?Enc(?:oded)?",
    // Feature markers
    "3D", "HSBS", "HOU", "SBS",
    "Dual[\\._-]?Audio", "Multi",
    "Wide-?screen", "WS",
    "Hybrid",
    // Content markers
    "Complete", "Bonus", "Extras?",
    "Trailer", "Sample", "Proof",
    "Documentary", "DOCU", "DOKU",
    // Release markers
    "Internal", "Classic", "Retail",
    "Obfuscated", "Scrambled",
    "Read-?NFO",
    "Colorized",
    "Converted", "CONVERT",
    "Mux",
    "Upscaled?",
    // Video standards
    "PAL", "SECAM", "NTSC",
    // Frame rate
    "HFR", "VFR",
    // Quality tiers
    "HQ", "HR", "MD",
    "mHD", "HDLight", "Micro-?HD",
    "LDTV",
    // Hardcoded subtitles
    "HC", "VOST",
    // Streaming services (common ones)
    "NF", "Netflix", "AMZN", "Amazon", "HMAX", "DSNP", "Disney\\+?",
    "ATVP", "ATV\\+?", "APTV",
    "HULU", "PMTP", "HBO",
    "iP", "BBC-?iPlayer",
    "CR", "Crunchy-?Roll",
    "STAN", "BCORE",
    // Subtitle groups / release groups marker
    "DIY", "FanSub", "FastSub",
    // Misc abbreviations that appear in release names
    "MiniBD", "Rarbg", "YIFY", "YTS",
    "SPARKS", "GECKOS", "AMIABLE", "LOL", "FGT",
    "CMCT", "CMCTV", "CHD", "CHDBits", "HDChina", "HDHome",
    "PTHome", "PTer", "OurBits", "MTeam",
    "EtHD", "WiKi", "beAst",
    "TorrenTGui", "TRiToN", "nLiBRA",
    "DiY@HDHome", "DiY@Audies", "DiY@PTHome",
    "DIY@HDHome", "DIY@Audies", "DIY@PTHome",
    "doraemon", "AJP69", "CALiGARi",
    "FRENCh", "FRENCH", "MULTi", "MULTI",
    "GBR", "USA", "JPN", "CHN", "HKG", "FRE", "GER", "ITA", "SPA", "KOR",
    "CEE", "EUR", "TWN", "RUS",
    // Country / region codes (3-letter, after year typically)
    "SDR",
];

/// Build one giant regex that matches any known technical token.
/// Patterns are joined with `|`, wrapped in word boundaries, case-insensitive.
fn build_token_regex(patterns: &[&str]) -> Regex {
    let joined = patterns.join("|");
    Regex::new(&format!(r"(?i)(?:^|[\s.\-_/()\[\]@])(?:{})(?:[\s.\-_/()\[\]@]|$)", joined))
        .unwrap()
}

/// Combined mega-regex for all technical tokens.
static NOISE_RE: Lazy<Regex> = Lazy::new(|| {
    let mut all: Vec<&str> = Vec::new();
    all.extend_from_slice(SOURCES);
    all.extend_from_slice(VIDEO_CODECS);
    all.extend_from_slice(SCREEN_SIZES);
    all.extend_from_slice(AUDIO);
    all.extend_from_slice(CONTAINERS);
    all.extend_from_slice(EDITIONS);
    all.extend_from_slice(OTHER);
    build_token_regex(&all)
});

/// Year pattern: 1900-2099 — uses word boundary which doesn't consume characters
static YEAR_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\b(?P<year>(?:19|20)\d{2})\b").unwrap()
});

/// Bracketed content: [anything] or (anything)
static BRACKET_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\[[^\]]*\]|\([^)]*\)|\{[^}]*\}").unwrap()
});

/// Release group: dash-separated suffix at end, e.g. "-GROUP" or "-GROUP@Site"
static RELEASE_GROUP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"[-][\w@]+$").unwrap()
});

/// Multiple spaces / separators
static MULTI_SPACE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\s{2,}").unwrap()
});

/// Trailing noise: leading/trailing dashes, dots, underscores, spaces
static TRIM_CHARS: &[char] = &[' ', '-', '_', '.', ',', ';', ':', '(', ')', '[', ']', '{', '}'];

pub fn parse_directory_name(name: &str) -> ParsedName {
    // Step 1: Normalize separators FIRST (before bracket removal so we can find years in parens)
    let normalized = normalize_separators(name);

    // Step 2: Extract year from the FULL string (before bracket removal).
    // This catches years like "(1999)" inside parens.
    let year = extract_best_year(&normalized);

    // Step 3: Remove bracketed content
    let mut working = BRACKET_RE.replace_all(&normalized, " ").into_owned();

    // Step 4: If year found, try the "truncate after year" strategy.
    if let Some(y) = year {
        let year_str = y.to_string();
        // Find the FIRST occurrence of this year in working text
        if let Some(pos) = find_year_occurrence(&working, &year_str) {
            let before_year = working[..pos].trim_matches(TRIM_CHARS);
            if !before_year.is_empty() {
                let title_raw = clean_final_title(before_year);
                if !title_raw.is_empty() {
                    let (title, alt_title) = extract_titles(&title_raw);
                    return ParsedName { title, alt_title, year: Some(y) };
                }
            }
            // Year is at the start with nothing before it.
            // The text after the year IS the rest — if it's all tech noise,
            // the year itself (or text before it in original) is the title.
            let after_year = working[pos + year_str.len()..].trim();
            if !after_year.is_empty() {
                // Strip all noise from after-year text
                let mut cleaned_after = after_year.to_string();
                cleaned_after = BRACKET_RE.replace_all(&cleaned_after, " ").into_owned();
                for _ in 0..3 {
                    let prev = cleaned_after.clone();
                    cleaned_after = strip_noise_tokens(&cleaned_after);
                    if cleaned_after == prev { break; }
                }
                cleaned_after = RELEASE_GROUP_RE.replace(&cleaned_after, "").into_owned();
                let remaining = clean_final_title(&cleaned_after);
                if remaining.is_empty() {
                    // Everything after year was noise — year is at start,
                    // check if there was a title before the year in the ORIGINAL text
                    // This handles "1917 2019 UHD..." → look for text in original before any year
                    // Nothing before → the numeric string before year is the title
                    // Actually, we need to check: is there text BEFORE year in the full string?
                    // For "1917 2019 UHD..." with y=2019, pos is the position of 2019
                    // before_year would be "1917" which is not empty → handled above
                    // This path is for when year is truly at position 0
                }
            }
        }
    }

    // Step 5: No year or truncation didn't work — strip everything
    for _ in 0..3 {
        let prev = working.clone();
        working = strip_noise_tokens(&working);
        if working == prev {
            break;
        }
    }

    // Remove release group suffix
    working = RELEASE_GROUP_RE.replace(&working, "").into_owned();

    // Remove year from title if present
    if let Some(y) = year {
        working = working.replace(&y.to_string(), " ");
    }

    let title_raw = clean_final_title(&working);
    let (title, alt_title) = extract_titles(&title_raw);

    ParsedName { title, alt_title, year }
}

/// Normalize dots and underscores to spaces, handling abbreviations.
fn normalize_separators(name: &str) -> String {
    let mut result = name.to_string();

    // Replace underscores with spaces
    result = result.replace('_', " ");

    // Replace dots with spaces, but preserve:
    // - Decimal numbers (e.g., "5.1")
    // - Abbreviations like S.H.I.E.L.D (single char between dots)
    let chars: Vec<char> = result.chars().collect();
    let mut normalized = String::with_capacity(result.len());
    let len = chars.len();

    for i in 0..len {
        if chars[i] == '.' {
            // Check if this is part of a version/channel number (e.g., 5.1, 2.0)
            let prev_digit = i > 0 && chars[i - 1].is_ascii_digit();
            let next_digit = i + 1 < len && chars[i + 1].is_ascii_digit();
            if prev_digit && next_digit {
                // Check if it's a channel format like 5.1, 7.1, 2.0
                // Keep the dot for these
                normalized.push('.');
            } else {
                normalized.push(' ');
            }
        } else {
            normalized.push(chars[i]);
        }
    }

    normalized
}

/// Extract the best year from text.
/// Finds all year candidates and picks the one most likely to be the release year
/// (not part of the title like "2001" in "2001 A Space Odyssey").
fn extract_best_year(text: &str) -> Option<u16> {
    let mut candidates: Vec<(u16, usize)> = Vec::new(); // (year, position)

    for cap in YEAR_RE.captures_iter(text) {
        if let Some(m) = cap.name("year") {
            if let Ok(y) = m.as_str().parse::<u16>() {
                if (1920..=2099).contains(&y) {
                    candidates.push((y, m.start()));
                }
            }
        }
    }

    if candidates.is_empty() {
        return None;
    }

    // If only one candidate, use it
    if candidates.len() == 1 {
        return Some(candidates[0].0);
    }

    // Multiple candidates: prefer the LAST year that has non-empty text before it.
    // Why last? In "Blade Runner 2049 2017 1080p", 2017 is the release year.
    // In "2046 2004 Criterion", 2004 is the release year.
    // The pattern is: Title [optional-year-in-title] ReleaseYear TechInfo
    for &(y, pos) in candidates.iter().rev() {
        if pos > 0 {
            let before = text[..pos].trim();
            if !before.is_empty() {
                return Some(y);
            }
        }
    }

    // Fallback: use the last candidate (most likely to be release year)
    Some(candidates.last().unwrap().0)
}

/// Find the position where year splits title from technical info.
/// Handles cases where the year value also appears as part of the title.
fn find_year_split_pos(text: &str, year_str: &str) -> Option<usize> {
    let year_val: u16 = year_str.parse().ok()?;
    let text_lower = text.to_lowercase();

    // Find all occurrences of this year in the text
    let mut positions: Vec<usize> = Vec::new();
    let mut start = 0;
    while let Some(pos) = text[start..].find(year_str) {
        let abs_pos = start + pos;
        // Must be at a word boundary
        let before_ok = abs_pos == 0 || !text.as_bytes()[abs_pos - 1].is_ascii_alphanumeric();
        let after_pos = abs_pos + year_str.len();
        let after_ok = after_pos >= text.len() || !text.as_bytes()[after_pos].is_ascii_alphanumeric();
        if before_ok && after_ok {
            positions.push(abs_pos);
        }
        start = abs_pos + 1;
    }

    if positions.is_empty() {
        return None;
    }

    // If there's only one occurrence, use it — but check if it's at the very start
    // with no preceding text (like "2046 2004 ..." where 2046 is the title).
    if positions.len() == 1 {
        let pos = positions[0];
        let before = text[..pos].trim();
        if before.is_empty() {
            // Year is at the start. Check if there's text after it that looks like a title.
            let after = text[pos + year_str.len()..].trim();
            // If the text after the year starts with another year, the first is the title.
            // e.g. "2046 2004 Criterion..." → 2046 is title, 2004 is year
            if let Some(cap) = YEAR_RE.captures(after) {
                if let Some(m) = cap.name("year") {
                    let next_year: u16 = m.as_str().parse().unwrap_or(0);
                    if next_year != year_val && (1920..=2099).contains(&next_year) {
                        // The second year is the real release year — but that's handled by
                        // extract_best_year. Here we return the position of our year.
                        return Some(pos);
                    }
                }
            }
            // Year at start, nothing before it — it might be part of the title.
            // Check if text after year looks like tech info.
            let after_words: Vec<&str> = after.split_whitespace().collect();
            if !after_words.is_empty() {
                let first_after = after_words[0];
                // If the word right after year is clearly tech (resolution, codec, source),
                // then this year IS the split point and the title is numeric.
                let is_tech = NOISE_RE.is_match(&format!(" {} ", first_after));
                if is_tech {
                    // e.g. "1917 2019 UHD BluRay..." — title is before year
                    // but before is empty, so we need a different strategy
                    return None; // let the caller handle it via full stripping
                }
            }
            return None;
        }
        return Some(pos);
    }

    // Multiple occurrences: prefer the one that's NOT at position 0
    // and has text before it (i.e., it separates title from tech info)
    for &pos in &positions {
        let before = text[..pos].trim();
        if !before.is_empty() {
            return Some(pos);
        }
    }

    Some(positions[0])
}

/// Find ALL years in text, returned in order of appearance.
fn find_all_years(text: &str) -> Vec<u16> {
    let mut years = Vec::new();
    for cap in YEAR_RE.captures_iter(text) {
        if let Some(m) = cap.name("year") {
            if let Ok(y) = m.as_str().parse::<u16>() {
                if (1920..=2099).contains(&y) {
                    years.push(y);
                }
            }
        }
    }
    years
}

/// Find the byte position of a year string in text (word-boundary aware).
fn find_year_occurrence(text: &str, year_str: &str) -> Option<usize> {
    let mut start = 0;
    while let Some(pos) = text[start..].find(year_str) {
        let abs_pos = start + pos;
        let before_ok = abs_pos == 0 || !text.as_bytes()[abs_pos - 1].is_ascii_alphanumeric();
        let after_pos = abs_pos + year_str.len();
        let after_ok = after_pos >= text.len() || !text.as_bytes()[after_pos].is_ascii_alphanumeric();
        if before_ok && after_ok {
            return Some(abs_pos);
        }
        start = abs_pos + 1;
    }
    None
}

/// Concatenated token pattern: catches things like "TrueHD7.1", "MiniBD1080P", "AAC1.0", "DTSHDMA"
static CONCAT_NOISE_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(?:TrueHD|DTS-?HD-?MA|DTSHDMA|DTS-?HD|MiniBD|AAC|DD|DDP)\d[\d.]*").unwrap()
});

/// Audio channel patterns that may be concatenated (e.g., "5.1" standalone)
static CHANNEL_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?:^|\s)(?:[257]\.[01]|[2568]ch|mono|stereo)(?:\s|$)").unwrap()
});

/// "3Audios" and similar count+noun patterns
static COUNT_NOUN_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\d+Audios?").unwrap()
});

/// Strip all noise tokens from text.
fn strip_noise_tokens(text: &str) -> String {
    let mut result = text.to_string();

    // First: catch concatenated patterns (before general noise)
    result = CONCAT_NOISE_RE.replace_all(&result, " ").into_owned();
    result = COUNT_NOUN_RE.replace_all(&result, " ").into_owned();

    // Use the combined regex to blank out matches
    let blanked = NOISE_RE.replace_all(&result, " ");
    result = blanked.into_owned();

    result
}

/// Final cleanup of the extracted title.
/// Given a raw title string, split into primary and alt title if it contains
/// both Chinese and English. Returns (primary, alt).
/// Primary is whichever is longer / more meaningful; alt is the other.
fn extract_titles(raw: &str) -> (String, Option<String>) {
    let cleaned = clean_cn_noise(raw);
    let (cn, en) = split_cn_en(&cleaned);

    match (cn.is_empty(), en.is_empty()) {
        (true, true) => (raw.to_string(), None),
        (true, false) => (en, None),
        (false, true) => (cn, None),
        (false, false) => {
            // Both exist — use English as primary (better for TMDB search),
            // Chinese as alt
            (en, Some(cn))
        }
    }
}

fn clean_final_title(text: &str) -> String {
    let mut title = text.to_string();

    // Collapse multiple spaces
    title = MULTI_SPACE_RE.replace_all(&title, " ").into_owned();

    // Trim junk characters from edges
    title = title.trim_matches(TRIM_CHARS).to_string();

    // Remove trailing single characters that are likely leftover separators
    while title.chars().count() > 2 {
        let last = match title.chars().last() {
            Some(c) => c,
            None => break,
        };
        let second_last = match title.chars().rev().nth(1) {
            Some(c) => c,
            None => break,
        };
        if second_last == ' ' && !last.is_alphanumeric() {
            // Remove last char and trailing spaces (handle multi-byte chars)
            let trim_pos = title.char_indices().rev().nth(1).map(|(i, _)| i).unwrap_or(0);
            title = title[..trim_pos].trim_end().to_string();
        } else {
            break;
        }
    }

    title
}

#[cfg(test)]
mod tests {
    use super::*;

    // Standard release names
    #[test]
    fn standard_dot_separated() {
        let p = parse_directory_name("The.Matrix.1999.1080p.BluRay.x264");
        assert_eq!(p.title, "The Matrix");
        assert_eq!(p.year, Some(1999));
    }

    #[test]
    fn with_group_suffix() {
        let p = parse_directory_name("Inception.2010.BluRay.1080p.x264-GROUP");
        assert_eq!(p.title, "Inception");
        assert_eq!(p.year, Some(2010));
    }

    #[test]
    fn chinese_title() {
        let p = parse_directory_name("让子弹飞.2010.1080p.BluRay");
        assert_eq!(p.title, "让子弹飞");
        assert_eq!(p.year, Some(2010));
    }

    #[test]
    fn underscore_separator() {
        let p = parse_directory_name("Blade_Runner_2049_2017_1080p");
        assert_eq!(p.title, "Blade Runner 2049");
        assert_eq!(p.year, Some(2017));
    }

    #[test]
    fn no_year() {
        let p = parse_directory_name("Some Movie Name");
        assert_eq!(p.title, "Some Movie Name");
        assert_eq!(p.year, None);
    }

    #[test]
    fn brackets_and_group() {
        let p = parse_directory_name("[YTS] The Matrix (1999) [1080p]");
        assert_eq!(p.title, "The Matrix");
        assert_eq!(p.year, Some(1999));
    }

    // Real-world examples from the user's library
    #[test]
    fn real_world_valiant_ones() {
        let p = parse_directory_name("The.Valiant.Ones.1975.Blu-ray.1080p.AVC.LPCM.2.0-DIY@HDHome.iso");
        assert_eq!(p.title, "The Valiant Ones");
        assert_eq!(p.year, Some(1975));
    }

    #[test]
    fn real_world_moment_of_romance() {
        let p = parse_directory_name("A Moment of Romance 1990 GBR Blu-ray 1080p AVC LPCM 2.0-doraemon");
        assert_eq!(p.title, "A Moment of Romance");
        assert_eq!(p.year, Some(1990));
    }

    #[test]
    fn real_world_bicycle_thieves() {
        let p = parse_directory_name("Ladri.di.biciclette.1948.1080p.BluRay.AAC1.0.-CALiGARi.mkv");
        assert_eq!(p.title, "Ladri di biciclette");
        assert_eq!(p.year, Some(1948));
    }

    #[test]
    fn real_world_turin_horse() {
        let p = parse_directory_name("The.Turin.Horse.2011.1080p.BluRay.AVC.LPCM.2.0-DIY@Audies");
        assert_eq!(p.title, "The Turin Horse");
        assert_eq!(p.year, Some(2011));
    }

    #[test]
    fn real_world_2001() {
        let p = parse_directory_name("2001.A.Space.Odyssey.1968.PROPER.2160p.UHD.Blu-ray.HEVC.DTS-HD.MA.5.1-TAiCHi");
        assert_eq!(p.title, "2001 A Space Odyssey");
        assert_eq!(p.year, Some(1968));
    }

    #[test]
    fn real_world_criterion() {
        let p = parse_directory_name("2046.2004.Criterion.Collection.1080p.Blu-ray.AVC.DTS-HD.MA.5.1-DiY@HDHome");
        assert_eq!(p.title, "2046");
        assert_eq!(p.year, Some(2004));
    }

    #[test]
    fn real_world_3_idiots() {
        let p = parse_directory_name("3 Idiots 2009 Blu-ray 1080p AVC DTSHDMA 5.1-TorrenTGui");
        assert_eq!(p.title, "3 Idiots");
        assert_eq!(p.year, Some(2009));
    }

    #[test]
    fn real_world_beautiful_mind() {
        let p = parse_directory_name("A.Beautiful.Mind.2001.720p.BluRay.x264.AAC-iHD.mp4");
        assert_eq!(p.title, "A Beautiful Mind");
        assert_eq!(p.year, Some(2001));
    }

    #[test]
    fn real_world_clockwork_orange() {
        let p = parse_directory_name("A.Clockwork.Orange.1971.2160p.UHD.Blu-ray.HEVC.DTS-HD.MA.5.1-DiY@HDHome");
        assert_eq!(p.title, "A Clockwork Orange");
        assert_eq!(p.year, Some(1971));
    }

    #[test]
    fn real_world_chinese_ghost_story() {
        let p = parse_directory_name("A Chinese Ghost Story 1987 CHN Blu-ray 1080p AVC DTS-HD MA 7.1-DIY@doraemon.iso");
        assert_eq!(p.title, "A Chinese Ghost Story");
        assert_eq!(p.year, Some(1987));
    }

    #[test]
    fn real_world_1917() {
        let p = parse_directory_name("1917 2019 UHD BluRay REMUX 2160p HEVC Atmos TrueHD7.1-CHD");
        assert_eq!(p.title, "1917");
        assert_eq!(p.year, Some(2019));
    }

    #[test]
    fn real_world_300() {
        let p = parse_directory_name("300.2007.Blu-ray.x264.TrueHD.5.1.3Audios.MiniBD1080P-CMCT");
        assert_eq!(p.title, "300");
        assert_eq!(p.year, Some(2007));
    }

    #[test]
    fn real_world_12_angry_men() {
        let p = parse_directory_name("12.Angry.Men.1957.Criterion.Collection.1080p.BluRay.AVC.LPCM.1.0-DIY@HDHome");
        assert_eq!(p.title, "12 Angry Men");
        assert_eq!(p.year, Some(1957));
    }

    #[test]
    fn real_world_fistful_of_dollars() {
        let p = parse_directory_name("A.Fistful.of.Dollars.1964.UHD.Blu-ray.2160p.HEVC.DTS-HD.MA.5.1-DiY@HDHome");
        assert_eq!(p.title, "A Fistful of Dollars");
        assert_eq!(p.year, Some(1964));
    }

    #[test]
    fn real_world_aftersun() {
        let p = parse_directory_name("Aftersun.2022.1080p.GBR.Blu-ray.AVC.DTS-HD.MA.5.1-DiY@HDHome");
        assert_eq!(p.title, "Aftersun");
        assert_eq!(p.year, Some(2022));
    }

    #[test]
    fn real_world_brighter_summer_day() {
        let p = parse_directory_name("A.Brighter.Summer.Day.1991.1080p.Criterion.Collection.Blu-ray.AVC.LPCM.1.0-nLiBRA");
        assert_eq!(p.title, "A Brighter Summer Day");
        assert_eq!(p.year, Some(1991));
    }

    #[test]
    fn real_world_ghibli_no_year_in_name() {
        // Year is at the start as part of title convention
        let p = parse_directory_name("1989 Kiki's Delivery Service");
        assert_eq!(p.title, "Kiki's Delivery Service");
        assert_eq!(p.year, Some(1989));
    }

    #[test]
    fn real_world_uhd_4k_bracket() {
        let p = parse_directory_name("[4K原盘DIY中字]猎杀红色十月.The.Hunt.for.Red.October.1990.2160p.UHD.Blu-ray.HEVC.TrueHD.5.1-A236P5@OurBits");
        assert_eq!(p.year, Some(1990));
        // Title should contain the movie name
        assert!(p.title.contains("Hunt for Red October") || p.title.contains("猎杀红色十月"));
    }

    #[test]
    fn real_world_kill_mockingbird() {
        let p = parse_directory_name("50th Anniversary Edition To Kill a Mockingbird CEE 1080p Bluray VC-1 DTS-HD MA 5.1 DVDSEED");
        // No year in name — harder case
        assert!(p.title.contains("Kill a Mockingbird") || p.title.contains("50th Anniversary"));
    }

    // Chinese+English mixed title tests
    #[test]
    fn mixed_cn_en_leon() {
        let p = parse_directory_name("这个杀手不太冷.Leon.The.Professional.1994.BluRay.2160p.x265.10bit.HDR.3Audio.mUHD-FRDS");
        assert_eq!(p.title, "Leon The Professional");
        assert_eq!(p.alt_title, Some("这个杀手不太冷".to_string()));
        assert_eq!(p.year, Some(1994));
    }

    #[test]
    fn mixed_cn_en_prague() {
        let p = parse_directory_name("布拉格之恋.The.Unbearable.Lightness.of.Being.1988.1080p.WEBRip.DD2.0.x264-NTb");
        assert_eq!(p.title, "The Unbearable Lightness of Being");
        assert_eq!(p.alt_title, Some("布拉格之恋".to_string()));
        assert_eq!(p.year, Some(1988));
    }

    #[test]
    fn mixed_cn_en_dust_of_time() {
        let p = parse_directory_name("时光之尘 The Dust of Time 2008 1080p JPN Blu-ray AVC TrueHD 5.1-DiY@SuperCinephile");
        assert_eq!(p.title, "The Dust of Time");
        assert_eq!(p.alt_title, Some("时光之尘".to_string()));
        assert_eq!(p.year, Some(2008));
    }

    #[test]
    fn mixed_cn_en_princess_bride() {
        let p = parse_directory_name("公主新娘 The Princess Bride 1987 UHD Blu-ray 2160p HEVC DTS-HD MA 5.1-Pete@HDSky");
        assert_eq!(p.title, "The Princess Bride");
        assert_eq!(p.alt_title, Some("公主新娘".to_string()));
        assert_eq!(p.year, Some(1987));
    }

    #[test]
    fn pure_chinese_title() {
        let p = parse_directory_name("大力水手");
        assert_eq!(p.title, "大力水手");
        assert_eq!(p.alt_title, None);
    }
}
