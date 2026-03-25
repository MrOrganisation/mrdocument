/// Truncate a string to at most `max_bytes` bytes at a valid UTF-8 char boundary.
pub fn truncate_str(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    // Walk backwards from max_bytes to find a char boundary
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ascii_within_limit() {
        assert_eq!(truncate_str("hello", 10), "hello");
    }

    #[test]
    fn test_ascii_at_limit() {
        assert_eq!(truncate_str("hello", 5), "hello");
    }

    #[test]
    fn test_ascii_over_limit() {
        assert_eq!(truncate_str("hello world", 5), "hello");
    }

    #[test]
    fn test_empty() {
        assert_eq!(truncate_str("", 10), "");
    }

    #[test]
    fn test_zero_limit() {
        assert_eq!(truncate_str("hello", 0), "");
    }

    #[test]
    fn test_umlaut_not_split() {
        // "ä" is 2 bytes (0xC3 0xA4)
        let s = "Erstgespräch";
        // "Erstgespr" = 9 bytes, "ä" = bytes 9-10, "ch" = bytes 11-12
        assert_eq!(truncate_str(s, 10), "Erstgespr");
        assert_eq!(truncate_str(s, 11), "Erstgesprä");
        assert_eq!(truncate_str(s, 9), "Erstgespr");
    }

    #[test]
    fn test_multibyte_boundary_exact() {
        let s = "aä"; // 'a' = 1 byte, 'ä' = 2 bytes, total = 3
        assert_eq!(truncate_str(s, 3), "aä");
        assert_eq!(truncate_str(s, 2), "a");
        assert_eq!(truncate_str(s, 1), "a");
    }

    #[test]
    fn test_german_text_truncate_at_30() {
        // "ä" spans bytes 29-30 — truncating at 30 must not split it
        let s = "Dokumenttyp Sitzung Erstgesprä und mehr";
        let result = truncate_str(s, 30);
        assert!(result.len() <= 30);
        assert!(s.starts_with(result));
        // Must back up to byte 29 (before the ä)
        assert_eq!(result, "Dokumenttyp Sitzung Erstgespr");
    }

    #[test]
    fn test_emoji() {
        // '😀' is 4 bytes
        let s = "hi 😀 there";
        assert_eq!(truncate_str(s, 4), "hi ");
        assert_eq!(truncate_str(s, 7), "hi 😀");
    }

    #[test]
    fn test_all_multibyte() {
        let s = "äöü"; // each 2 bytes = 6 total
        assert_eq!(truncate_str(s, 6), "äöü");
        assert_eq!(truncate_str(s, 5), "äö");
        assert_eq!(truncate_str(s, 4), "äö");
        assert_eq!(truncate_str(s, 3), "ä");
        assert_eq!(truncate_str(s, 2), "ä");
        assert_eq!(truncate_str(s, 1), "");
    }
}
