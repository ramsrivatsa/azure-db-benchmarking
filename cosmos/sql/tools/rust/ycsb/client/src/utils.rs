//! Helpers that reproduce Java behavior the YCSB output and key space depend on.

const FNV_OFFSET_BASIS_64: i64 = 0xCBF2_9CE4_8422_2325_u64 as i64;
const FNV_PRIME_64: i64 = 1_099_511_628_211;

/// `site.ycsb.Utils.fnvhash64`: FNV-1a over the 8 little-endian bytes, then `Math.abs`.
///
/// Key names are derived from this hash, so it must match Java bit for bit for a dataset
/// loaded by one implementation to be readable by the other.
pub fn fnvhash64(mut val: i64) -> i64 {
    let mut hashval = FNV_OFFSET_BASIS_64;
    for _ in 0..8 {
        let octet = val & 0x00ff;
        val >>= 8;
        hashval ^= octet;
        hashval = hashval.wrapping_mul(FNV_PRIME_64);
    }
    // `Math.abs(Long.MIN_VALUE)` stays negative in Java; `wrapping_abs` matches that.
    hashval.wrapping_abs()
}

/// `java.lang.String.hashCode()` over UTF-16 code units.
pub fn java_string_hash(s: &str) -> i32 {
    s.encode_utf16()
        .fold(0i32, |h, unit| h.wrapping_mul(31).wrapping_add(i32::from(unit)))
}

/// `Double.toString(double)`: plain notation in `[1e-3, 1e7)`, computerized scientific
/// notation (`1.0E7`) outside it, always at least one fractional digit.
pub fn java_double_to_string(d: f64) -> String {
    if d.is_nan() {
        return "NaN".to_string();
    }
    if d.is_infinite() {
        return if d > 0.0 { "Infinity" } else { "-Infinity" }.to_string();
    }
    if d == 0.0 {
        return if d.is_sign_negative() { "-0.0" } else { "0.0" }.to_string();
    }
    let magnitude = d.abs();
    if (1e-3..1e7).contains(&magnitude) {
        let mut s = format!("{d}");
        if !s.contains('.') {
            s.push_str(".0");
        }
        s
    } else {
        let formatted = format!("{d:e}");
        let (mantissa, exponent) = formatted
            .split_once('e')
            .expect("scientific formatting always contains an exponent");
        if mantissa.contains('.') {
            format!("{mantissa}E{exponent}")
        } else {
            format!("{mantissa}.0E{exponent}")
        }
    }
}

/// `new DecimalFormat("#.##").format(x)`: at most two fraction digits, HALF_EVEN rounding on
/// the exact binary value, trailing zeros dropped.
pub fn decimal_format_2(x: f64) -> String {
    if x.is_nan() {
        return "NaN".to_string();
    }
    if x.is_infinite() {
        return if x > 0.0 { "\u{221E}" } else { "-\u{221E}" }.to_string();
    }
    // Rust's fixed-precision formatting rounds the exact binary value half-to-even, which is
    // what DecimalFormat does since JDK 8.
    let mut s = format!("{x:.2}");
    if s.contains('.') {
        while s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
    }
    if s == "-0" {
        // DecimalFormat keeps the sign of negative values that round to zero.
        return "-0".to_string();
    }
    s
}

/// Order of `ConcurrentHashMap<String, _>` iteration for keys inserted in the given order.
///
/// YCSB prints measurements in map iteration order (which is why `[READ]` precedes `[CLEANUP]`
/// precedes `[UPDATE]`). Reproducing it keeps Rust and Java result files diff-friendly. Exact for
/// fewer than 12 keys (a single 16-bucket table); an approximation beyond that.
pub fn java_hash_map_order<T, F>(items: &mut [T], name_of: F)
where
    F: Fn(&T) -> &str,
{
    let mut capacity = 16usize;
    while items.len() >= capacity - (capacity >> 2) {
        capacity <<= 1;
    }
    let mask = (capacity - 1) as u32;
    // Stable sort keeps insertion order within a bucket, like a hash bin's linked list.
    items.sort_by_key(|item| {
        let h = java_string_hash(name_of(item)) as u32;
        ((h ^ (h >> 16)) & 0x7fff_ffff) & mask
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnvhash64_matches_java_ycsb_keys() {
        // Well-known first keys of every hashed YCSB load (`user` + fnvhash64(n)).
        assert_eq!(fnvhash64(0), 6_284_781_860_667_377_211);
        assert_eq!(fnvhash64(1), 8_517_097_267_634_966_620);
        assert_eq!(fnvhash64(2), 1_820_151_046_732_198_393);
    }

    #[test]
    fn fnvhash64_is_never_negative_for_typical_inputs() {
        for i in 0..100_000 {
            assert!(fnvhash64(i) >= 0);
        }
    }

    #[test]
    fn java_string_hash_matches_java() {
        assert_eq!(java_string_hash(""), 0);
        assert_eq!(java_string_hash("READ"), 2_511_254);
        assert_eq!(java_string_hash("hello"), 99_162_322);
        // Overflowing hash wraps like Java's int arithmetic.
        assert_eq!(
            java_string_hash("user6284781860667377211:field0"),
            java_string_hash_reference("user6284781860667377211:field0")
        );
    }

    fn java_string_hash_reference(s: &str) -> i32 {
        let mut h: i64 = 0;
        for unit in s.encode_utf16() {
            h = (31 * h + i64::from(unit)) & 0xffff_ffff;
        }
        h as u32 as i32
    }

    #[test]
    fn java_double_to_string_matches_java_formats() {
        assert_eq!(java_double_to_string(0.0), "0.0");
        assert_eq!(java_double_to_string(1.0), "1.0");
        assert_eq!(java_double_to_string(100.0), "100.0");
        assert_eq!(java_double_to_string(1234.5678), "1234.5678");
        assert_eq!(java_double_to_string(0.001), "0.001");
        assert_eq!(java_double_to_string(9_999_999.0), "9999999.0");
        assert_eq!(java_double_to_string(1e7), "1.0E7");
        assert_eq!(java_double_to_string(12_345_678.9), "1.23456789E7");
        assert_eq!(java_double_to_string(0.0001), "1.0E-4");
        assert_eq!(java_double_to_string(f64::NAN), "NaN");
        assert_eq!(java_double_to_string(f64::INFINITY), "Infinity");
        assert_eq!(java_double_to_string(1663.893510815308), "1663.893510815308");
    }

    #[test]
    fn decimal_format_2_matches_java_decimal_format() {
        assert_eq!(decimal_format_2(0.0), "0");
        assert_eq!(decimal_format_2(1574.14), "1574.14");
        assert_eq!(decimal_format_2(12876.194), "12876.19");
        assert_eq!(decimal_format_2(12876.196), "12876.2");
        assert_eq!(decimal_format_2(10.0), "10");
        assert_eq!(decimal_format_2(2.5), "2.5");
        // Exact binary ties round half-to-even like DecimalFormat.
        assert_eq!(decimal_format_2(0.125), "0.12");
        assert_eq!(decimal_format_2(0.375), "0.38");
        // 0.135 is slightly above the tie in binary, so it rounds up.
        assert_eq!(decimal_format_2(0.135), "0.14");
        assert_eq!(decimal_format_2(25807.0), "25807");
    }

    #[test]
    fn java_hash_map_order_matches_ycsb_output_order() {
        let mut ops = vec!["READ", "UPDATE", "CLEANUP"];
        java_hash_map_order(&mut ops, |s| s);
        assert_eq!(ops, vec!["READ", "CLEANUP", "UPDATE"]);

        let mut load = vec!["INSERT", "CLEANUP"];
        java_hash_map_order(&mut load, |s| s);
        assert_eq!(load, vec!["CLEANUP", "INSERT"]);
    }
}
