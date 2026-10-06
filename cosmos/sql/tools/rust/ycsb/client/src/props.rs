//! `java.util.Properties` compatible configuration.
//!
//! YCSB workload files and binding property files are Java properties files, and the
//! benchmarking scripts edit them with `sed`, so this parser follows `Properties.load`
//! closely: `=`, `:` or whitespace separators, `#`/`!` comments, backslash line
//! continuations and escapes, and values that keep their trailing whitespace.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{anyhow, Context, Result};

#[derive(Clone, Debug, Default)]
pub struct Properties {
    map: HashMap<String, String>,
}

impl Properties {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn load_file(&mut self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        let bytes =
            std::fs::read(path).with_context(|| format!("Unable to open the properties file {}", path.display()))?;
        // `Properties.load(InputStream)` decodes ISO-8859-1: every byte is one char.
        let text: String = bytes.iter().map(|&b| char::from(b)).collect();
        self.load_str(&text);
        Ok(())
    }

    pub fn load_str(&mut self, text: &str) {
        for line in logical_lines(text) {
            let (key, value) = split_key_value(&line);
            self.map.insert(key, value);
        }
    }

    pub fn set(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.map.insert(key.into(), value.into());
    }

    /// Copies every entry of `other` over this set (later sources win, as in YCSB).
    pub fn merge_from(&mut self, other: &Properties) {
        for (k, v) in &other.map {
            self.map.insert(k.clone(), v.clone());
        }
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.map.get(key).map(String::as_str)
    }

    pub fn contains(&self, key: &str) -> bool {
        self.map.contains_key(key)
    }

    pub fn get_or<'a>(&'a self, key: &str, default: &'a str) -> &'a str {
        self.get(key).unwrap_or(default)
    }

    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.map.keys().map(String::as_str)
    }

    /// `Integer.parseInt(getProperty(key, default))`, reporting a YCSB-style error.
    pub fn parse_i32(&self, key: &str, default: &str) -> Result<i32> {
        let raw = self.get_or(key, default);
        parse_java_int(raw).ok_or_else(|| anyhow!("Invalid integer for property {key}: \"{raw}\""))
    }

    /// `Long.parseLong(getProperty(key, default))`.
    pub fn parse_i64(&self, key: &str, default: &str) -> Result<i64> {
        let raw = self.get_or(key, default);
        parse_java_long(raw).ok_or_else(|| anyhow!("Invalid long for property {key}: \"{raw}\""))
    }

    /// `Double.parseDouble(getProperty(key, default))`.
    pub fn parse_f64(&self, key: &str, default: &str) -> Result<f64> {
        let raw = self.get_or(key, default);
        parse_java_double(raw).ok_or_else(|| anyhow!("Invalid double for property {key}: \"{raw}\""))
    }

    /// `Boolean.parseBoolean(getProperty(key, default))`: true only for "true", any case.
    pub fn parse_bool(&self, key: &str, default: bool) -> bool {
        match self.get(key) {
            Some(raw) => raw.eq_ignore_ascii_case("true"),
            None => default,
        }
    }

    /// The Cosmos binding's `getIntProperty`: unparsable values fall back to the default.
    pub fn int_or_default(&self, key: &str, default: i32) -> i32 {
        self.get(key).and_then(parse_java_int).unwrap_or(default)
    }
}

/// `Integer.parseInt`: optional sign, ASCII digits, no surrounding whitespace.
pub fn parse_java_int(s: &str) -> Option<i32> {
    if !is_java_integer_literal(s) {
        return None;
    }
    s.parse::<i32>().ok()
}

/// `Long.parseLong`.
pub fn parse_java_long(s: &str) -> Option<i64> {
    if !is_java_integer_literal(s) {
        return None;
    }
    s.parse::<i64>().ok()
}

fn is_java_integer_literal(s: &str) -> bool {
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
}

/// `Double.parseDouble`: trims whitespace and accepts a trailing `d`/`f` type suffix.
pub fn parse_java_double(s: &str) -> Option<f64> {
    let trimmed = s.trim();
    let body = trimmed
        .strip_suffix(['d', 'D', 'f', 'F'])
        .filter(|b| !b.is_empty() && !b.ends_with(['e', 'E']))
        .unwrap_or(trimmed);
    match body {
        "NaN" => Some(f64::NAN),
        "Infinity" | "+Infinity" => Some(f64::INFINITY),
        "-Infinity" => Some(f64::NEG_INFINITY),
        _ if body.eq_ignore_ascii_case("nan") || body.to_ascii_lowercase().contains("inf") => None,
        _ => body.parse::<f64>().ok(),
    }
}

/// Splits text into logical lines: drops blank and comment lines, joins continuations.
fn logical_lines(text: &str) -> Vec<String> {
    let mut lines = Vec::new();
    let mut current = String::new();
    let mut continuing = false;
    for raw in text.split('\n') {
        let raw = raw.strip_suffix('\r').unwrap_or(raw);
        let trimmed_start = raw.trim_start_matches([' ', '\t', '\u{c}']);
        if !continuing && (trimmed_start.is_empty() || trimmed_start.starts_with(['#', '!'])) {
            continue;
        }
        let piece = trimmed_start;
        // A line continues when it ends with an odd number of backslashes.
        let trailing_backslashes = piece.chars().rev().take_while(|&c| c == '\\').count();
        if trailing_backslashes % 2 == 1 {
            current.push_str(&piece[..piece.len() - 1]);
            continuing = true;
        } else {
            current.push_str(piece);
            lines.push(std::mem::take(&mut current));
            continuing = false;
        }
    }
    if continuing && !current.is_empty() {
        lines.push(current);
    }
    lines
}

fn split_key_value(line: &str) -> (String, String) {
    let chars: Vec<char> = line.chars().collect();
    let mut key_end = chars.len();
    let mut value_start = chars.len();
    let mut has_separator = false;
    let mut preceding_backslash = false;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if (c == '=' || c == ':') && !preceding_backslash {
            key_end = i;
            value_start = i + 1;
            has_separator = true;
            break;
        } else if (c == ' ' || c == '\t' || c == '\u{c}') && !preceding_backslash {
            key_end = i;
            value_start = i + 1;
            break;
        }
        preceding_backslash = c == '\\' && !preceding_backslash;
        i += 1;
    }
    while value_start < chars.len() {
        let c = chars[value_start];
        if c == ' ' || c == '\t' || c == '\u{c}' {
            value_start += 1;
            continue;
        }
        if !has_separator && (c == '=' || c == ':') {
            has_separator = true;
            value_start += 1;
            continue;
        }
        break;
    }
    let key: String = chars[..key_end].iter().collect();
    let value: String = chars[value_start.min(chars.len())..].iter().collect();
    (unescape(&key), unescape(&value))
}

fn unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('f') => out.push('\u{c}'),
            Some('u') => {
                let hex: String = chars.by_ref().take(4).collect();
                match u32::from_str_radix(&hex, 16).ok().and_then(char::from_u32) {
                    Some(decoded) => out.push(decoded),
                    None => {
                        out.push('u');
                        out.push_str(&hex);
                    }
                }
            }
            Some(other) => out.push(other),
            None => {}
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Properties {
        let mut p = Properties::new();
        p.load_str(text);
        p
    }

    #[test]
    fn load_str_parses_separators_and_comments() {
        let p = parse(
            "# comment\n! also comment\n\nrecordcount=1000\nworkload = site.ycsb.workloads.CoreWorkload\n\
             azurecosmos.uri = https://acct.documents.azure.com:443/\nkey:value\nspaced value here\n",
        );
        assert_eq!(p.get("recordcount"), Some("1000"));
        assert_eq!(p.get("workload"), Some("site.ycsb.workloads.CoreWorkload"));
        assert_eq!(p.get("azurecosmos.uri"), Some("https://acct.documents.azure.com:443/"));
        assert_eq!(p.get("key"), Some("value"));
        assert_eq!(p.get("spaced"), Some("value here"));
        assert_eq!(p.get("comment"), None);
    }

    #[test]
    fn load_str_ignores_commented_sed_placeholders() {
        let p = parse("# azurecosmos.primaryKey =\n# azurecosmos.useGateway = false\n");
        assert!(!p.contains("azurecosmos.primaryKey"));
        assert!(!p.contains("azurecosmos.useGateway"));
    }

    #[test]
    fn load_str_keeps_base64_key_characters() {
        let key = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
        let p = parse(&format!("azurecosmos.primaryKey = {key}\n"));
        assert_eq!(p.get("azurecosmos.primaryKey"), Some(key));
    }

    #[test]
    fn load_str_handles_continuations_and_escapes() {
        let p = parse("multi = first \\\n    second\nescaped\\ key = a\\tb\\u0041\nempty=\n");
        assert_eq!(p.get("multi"), Some("first second"));
        assert_eq!(p.get("escaped key"), Some("a\tbA"));
        assert_eq!(p.get("empty"), Some(""));
    }

    #[test]
    fn load_str_later_entries_win_and_trailing_space_is_kept() {
        let p = parse("a=1\na=2\nb=x  \n");
        assert_eq!(p.get("a"), Some("2"));
        assert_eq!(p.get("b"), Some("x  "));
    }

    #[test]
    fn load_str_handles_crlf_line_endings() {
        let p = parse("a=1\r\nb=2\r\n");
        assert_eq!(p.get("a"), Some("1"));
        assert_eq!(p.get("b"), Some("2"));
    }

    #[test]
    fn parse_java_int_follows_integer_parse_int() {
        assert_eq!(parse_java_int("42"), Some(42));
        assert_eq!(parse_java_int("-7"), Some(-7));
        assert_eq!(parse_java_int("+7"), Some(7));
        assert_eq!(parse_java_int(" 7"), None);
        assert_eq!(parse_java_int("7 "), None);
        assert_eq!(parse_java_int("1e3"), None);
        assert_eq!(parse_java_int("2147483648"), None);
    }

    #[test]
    fn parse_java_double_follows_double_parse_double() {
        assert_eq!(parse_java_double("0.95"), Some(0.95));
        assert_eq!(parse_java_double(" 1 "), Some(1.0));
        assert_eq!(parse_java_double("1e3"), Some(1000.0));
        assert_eq!(parse_java_double("2.5d"), Some(2.5));
        assert_eq!(parse_java_double("abc"), None);
        assert!(parse_java_double("NaN").unwrap().is_nan());
    }

    #[test]
    fn parse_bool_is_true_only_for_true() {
        let p = parse("a=TRUE\nb=yes\nc=true \n");
        assert!(p.parse_bool("a", false));
        assert!(!p.parse_bool("b", true));
        assert!(!p.parse_bool("c", true));
        assert!(p.parse_bool("missing", true));
    }

    #[test]
    fn int_or_default_falls_back_on_garbage() {
        let p = parse("a=12\nb=oops\n");
        assert_eq!(p.int_or_default("a", -1), 12);
        assert_eq!(p.int_or_default("b", -1), -1);
        assert_eq!(p.int_or_default("c", -1), -1);
    }
}
