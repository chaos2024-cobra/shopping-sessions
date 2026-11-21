//! Timestamp parsing per rules.md: three forms, strict ASCII, calendar checks.
//! Everything returns epoch milliseconds UTC.

pub const SECOND_MS: i64 = 1_000;
pub const MINUTE_MS: i64 = 60 * SECOND_MS;
pub const HOUR_MS: i64 = 60 * MINUTE_MS;
pub const DAY_MS: i64 = 24 * HOUR_MS;
pub const WEEK_MS: i64 = 7 * DAY_MS;

/// Days from 1970-01-01 to the given civil date (Howard Hinnant's algorithm).
#[inline]
pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

#[inline]
fn is_leap(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

#[inline]
fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

#[inline(always)]
fn dig(b: u8) -> Option<i64> {
    if b.is_ascii_digit() {
        Some((b - b'0') as i64)
    } else {
        None
    }
}

/// Strict F1: `YYYY-MM-DDTHH:MM:SSZ`, exactly 20 bytes.
#[inline]
pub fn parse_f1(s: &[u8]) -> Option<i64> {
    if s.len() != 20 {
        return None;
    }
    if s[4] != b'-'
        || s[7] != b'-'
        || s[10] != b'T'
        || s[13] != b':'
        || s[16] != b':'
        || s[19] != b'Z'
    {
        return None;
    }
    let y = dig(s[0])? * 1000 + dig(s[1])? * 100 + dig(s[2])? * 10 + dig(s[3])?;
    let mo = dig(s[5])? * 10 + dig(s[6])?;
    let d = dig(s[8])? * 10 + dig(s[9])?;
    let h = dig(s[11])? * 10 + dig(s[12])?;
    let mi = dig(s[14])? * 10 + dig(s[15])?;
    let se = dig(s[17])? * 10 + dig(s[18])?;
    let (mo, d, h, mi, se) = (mo, d, h, mi, se);
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || se > 59
    {
        return None;
    }
    Some((days_from_civil(y, mo, d) * 86_400 + h * 3_600 + mi * 60 + se) * 1000)
}

/// Strict F2: `YYYY-MM-DDTHH:MM:SS±HH:MM`, exactly 25 bytes, offset hours 00..23,
/// minutes 00..59. The offset is subtracted to get UTC.
#[inline]
pub fn parse_f2(s: &[u8]) -> Option<i64> {
    if s.len() != 25 {
        return None;
    }
    if s[4] != b'-'
        || s[7] != b'-'
        || s[10] != b'T'
        || s[13] != b':'
        || s[16] != b':'
        || (s[19] != b'+' && s[19] != b'-')
        || s[22] != b':'
    {
        return None;
    }
    let y = dig(s[0])? * 1000 + dig(s[1])? * 100 + dig(s[2])? * 10 + dig(s[3])?;
    let mo = dig(s[5])? * 10 + dig(s[6])?;
    let d = dig(s[8])? * 10 + dig(s[9])?;
    let h = dig(s[11])? * 10 + dig(s[12])?;
    let mi = dig(s[14])? * 10 + dig(s[15])?;
    let se = dig(s[17])? * 10 + dig(s[18])?;
    let oh = dig(s[20])? * 10 + dig(s[21])?;
    let om = dig(s[23])? * 10 + dig(s[24])?;
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || se > 59
    {
        return None;
    }
    if oh > 23 || om > 59 {
        return None;
    }
    let local = (days_from_civil(y, mo, d) * 86_400 + h * 3_600 + mi * 60 + se) * 1000;
    let offset = (oh * 3_600 + om * 60) * 1000;
    Some(if s[19] == b'+' {
        local - offset
    } else {
        local + offset
    })
}

/// F3: ASCII digits only, epoch milliseconds, leading zeros allowed.
/// Values that do not fit in i64 are treated as out of range (None).
#[inline]
pub fn parse_f3(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    let mut i = 0;
    // skip leading zeros
    while i < s.len() && s[i] == b'0' {
        i += 1;
    }
    if s.len() - i > 18 {
        return None;
    }
    let mut v: i64 = 0;
    while i < s.len() {
        let b = s[i];
        if !b.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (b - b'0') as i64;
        i += 1;
    }
    Some(v)
}

/// A `ts` field in any of the three forms. None means a bad timestamp.
#[inline]
pub fn parse_ts(s: &[u8]) -> Option<i64> {
    match s.len() {
        20 => match parse_f1(s) {
            Some(v) => Some(v),
            None => parse_f3(s),
        },
        25 => match parse_f2(s) {
            Some(v) => Some(v),
            None => parse_f3(s),
        },
        _ => parse_f3(s),
    }
}

/// `price_cents`: ASCII digits only with value 1..=10_000_000.
#[inline]
pub fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut i = 0;
    while i < s.len() && s[i] == b'0' {
        i += 1;
    }
    if s.len() - i > 8 {
        // More than 8 significant digits: > 10_000_000 for sure.
        // Still verify the whole field is digits so callers can rely on it.
        let mut j = i;
        while j < s.len() {
            if !s[j].is_ascii_digit() {
                return None;
            }
            j += 1;
        }
        return None;
    }
    let mut v: u64 = 0;
    let mut j = 0;
    while j < s.len() {
        let b = s[j];
        if !b.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (b - b'0') as u64;
        j += 1;
    }
    if (1..=10_000_000).contains(&v) {
        Some(v as u32)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forms() {
        assert_eq!(parse_ts(b"2026-10-03T23:58:10Z"), Some(1791071890000));
        assert_eq!(parse_ts(b"2026-10-03T19:58:10-04:00"), Some(1791071890000));
        assert_eq!(parse_ts(b"1791071890000"), Some(1791071890000));
        assert_eq!(parse_ts(b"0001791071890000"), Some(1791071890000));
        assert_eq!(parse_ts(b""), None);
        assert_eq!(parse_ts(b"1791071890000.0"), None);
        assert_eq!(parse_ts(b"1.79e12"), None);
        assert_eq!(parse_ts(b"-5"), None);
        assert_eq!(parse_ts(b"2026-10-03t23:58:10Z"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10z"), None);
        assert_eq!(parse_ts(b"2026-10-03 23:58:10Z"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10.5Z"), None);
        assert_eq!(parse_ts(b"2026-13-03T23:58:10Z"), None);
        assert_eq!(parse_ts(b"2026-02-30T23:58:10Z"), None);
        assert_eq!(parse_ts(b"2027-02-29T23:58:10Z"), None);
        assert_eq!(parse_ts(b"2028-02-29T23:58:10Z"), Some(1835481490000));
        assert_eq!(parse_ts(b"2026-10-03T24:58:10Z"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:60Z"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10+24:00"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10+05:60"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10+0200"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10-00:00"), Some(1791071890000));
        assert_eq!(parse_ts(b"2026-10-03T23:58:10+23:59"), Some(1790985550000));
        assert_eq!(parse_ts(b"86400000"), Some(86_400_000));
        assert_eq!(
            parse_ts(b"179102160000000000000"),
            None,
            "21 digits overflows"
        );
        assert_eq!(parse_ts("٢٥٠".as_bytes()), None);
        assert_eq!(parse_ts("２50".as_bytes()), None);
        // 20 chars, all digits -> F3
        assert_eq!(parse_ts(b"17910216000000000000"), None);
    }

    #[test]
    fn prices() {
        assert_eq!(parse_price(b"250"), Some(250));
        assert_eq!(parse_price(b"0250"), Some(250));
        assert_eq!(parse_price(b"1"), Some(1));
        assert_eq!(parse_price(b"10000000"), Some(10_000_000));
        assert_eq!(parse_price(b"10000001"), None);
        assert_eq!(parse_price(b"0"), None);
        assert_eq!(parse_price(b"00"), None);
        assert_eq!(parse_price(b""), None);
        assert_eq!(parse_price(b"19.99"), None);
        assert_eq!(parse_price(b"-5"), None);
        assert_eq!(parse_price(b"+500"), None);
        assert_eq!(parse_price(b"1e3"), None);
        assert_eq!(parse_price(b"250000000"), None);
        assert_eq!(parse_price("２５０".as_bytes()), None);
        assert_eq!(parse_price("٢٥٠".as_bytes()), None);
        assert_eq!(parse_price(b"000000000001"), Some(1));
    }
}
