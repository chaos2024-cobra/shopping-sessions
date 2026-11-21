//! SWAR CSV field splitting: find comma and quote bytes with 8-byte words,
//! then unquote fields with an inline interior scan (no memchr on the hot
//! path).

/// Byte-mask of bytes equal to `b` in the word. Carry-free exact form: the
/// classic (m - 0x01..) trick has phantom bits for 0x01-after-0x00, which
/// would turn `,-` into a phantom comma.
#[inline(always)]
fn eq_mask(w: u64, b: u8) -> u64 {
    let r = (b as u64) * 0x0101_0101_0101_0101;
    let m = w ^ r;
    !(((m & 0x7F7F_7F7F_7F7F_7F7F) + 0x7F7F_7F7F_7F7F_7F7F) | m) & 0x8080_8080_8080_8080
}

/// A field's value after CSV unquoting, or None when a quote appears anywhere
/// except wrapped around the whole field.
#[inline(always)]
pub fn unquote(f: &[u8]) -> Option<&[u8]> {
    if f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' {
        if memchr::memchr(b'"', &f[1..f.len() - 1]).is_some() {
            None
        } else {
            Some(&f[1..f.len() - 1])
        }
    } else if memchr::memchr(b'"', f).is_some() {
        None
    } else {
        Some(f)
    }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn quote_word_bits(line: &[u8], commas: &mut [usize; 15], want: usize, nfound: &mut usize) -> (u32, u32) {
    // returns (quote_words, nwords); fills comma positions; None-ish via nfound == usize::MAX on overflow
    use core::arch::x86_64::*;
    let len = line.len();
    let vc = unsafe { _mm_set1_epi8(b',' as i8) };
    let vq = unsafe { _mm_set1_epi8(b'"' as i8) };
    let mut quote_words: u32 = 0;
    let mut nwords = 0u32;
    let mut i = 0usize;
    while i + 16 <= len {
        let w = unsafe { _mm_loadu_si128(line.as_ptr().add(i) as *const __m128i) };
        let cm = unsafe { _mm_movemask_epi8(_mm_cmpeq_epi8(w, vc)) as u32 };
        let qm = unsafe { _mm_movemask_epi8(_mm_cmpeq_epi8(w, vq)) as u32 };
        if nwords < 32 && qm != 0 {
            quote_words |= 1 << nwords;
        }
        if cm != 0 {
            let mut m = cm;
            loop {
                let bit = m.trailing_zeros() as usize;
                if *nfound == want {
                    *nfound = usize::MAX; // too many fields
                    return (quote_words, nwords);
                }
                commas[*nfound] = i + bit;
                *nfound += 1;
                m &= m - 1;
                if m == 0 {
                    break;
                }
            }
        }
        i += 16;
        nwords += 1;
    }
    (quote_words, nwords)
}

/// Split `line` into exactly N comma-separated fields, applying the quoting
/// rules. Returns None for a wrong field count or malformed quoting.
#[cfg(target_arch = "x86_64")]
const WORD: usize = 16;
#[cfg(not(target_arch = "x86_64"))]
const WORD: usize = 8;

#[inline(always)]
pub fn split_n<'a, const N: usize>(line: &'a [u8]) -> Option<[&'a [u8]; N]> {
    debug_assert!(N >= 2 && N <= 16);
    let want = N - 1;
    let mut commas = [0usize; 15];
    let mut nfound = 0usize;
    // one bit per 8-byte word: the word contains a quote byte
    let mut quote_words: u32 = 0;
    let mut nwords = 0u32;

    let len = line.len();
    let mut i;
    #[cfg(target_arch = "x86_64")]
    {
        let (qw, nw) = quote_word_bits(line, &mut commas, want, &mut nfound);
        quote_words = qw;
        nwords = nw;
        if nfound == usize::MAX {
            return None; // too many fields
        }
        i = (nwords as usize) * 16;
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        i = 0;
        while i + 8 <= len {
            let w = u64::from_le_bytes(line[i..i + 8].try_into().unwrap());
            if nwords < 32 && eq_mask(w, b'"') != 0 {
                quote_words |= 1 << nwords;
            }
            let mut c = eq_mask(w, b',');
            if c != 0 {
                loop {
                    let bit = c.trailing_zeros() as usize;
                    let pos = i + (bit >> 3);
                    if nfound == want {
                        return None; // too many fields
                    }
                    commas[nfound] = pos;
                    nfound += 1;
                    c &= c - 1;
                    if c == 0 {
                        break;
                    }
                }
            }
            i += 8;
            nwords += 1;
        }
    }
    let mut tail_quote = false;
    while i < len {
        let b = line[i];
        if b == b',' {
            if nfound == want {
                return None;
            }
            commas[nfound] = i;
            nfound += 1;
        } else if b == b'"' {
            tail_quote = true;
        }
        i += 1;
    }
    if nfound != want {
        return None;
    }
    // the trailing partial word counts as word `nwords`
    if tail_quote && nwords < 32 {
        quote_words |= 1 << nwords;
    }

    let mut out: [&'a [u8]; N] = [&[]; N];
    let mut prev = 0usize;
    for k in 0..want {
        let cpos = commas[k];
        out[k] = &line[prev..cpos];
        prev = cpos + 1;
    }
    out[N - 1] = &line[prev..];

    // Unquote fields that overlap a quote word. `spans_quote([a,b))` tells
    // whether the byte range touches a word known to hold a quote byte;
    // ranges at or beyond word 32 are treated as overlapping (conservative).
    let spans_quote = |a: usize, b: usize| -> bool {
        if a >= b {
            return false;
        }
        let sw = a / WORD;
        let ew = (b - 1) / WORD;
        if ew >= 32 {
            return true;
        }
        let mask = (((1u64 << (ew - sw + 1)) - 1) << sw) as u32;
        quote_words & mask != 0
    };

    let mut raw_start = 0usize;
    for (k, f) in out.iter_mut().enumerate() {
        let raw_end = if k < want { commas[k] } else { len };
        if spans_quote(raw_start, raw_end) {
            // the field contains a quote byte somewhere
            if raw_end - raw_start >= 2
                && line[raw_start] == b'"'
                && line[raw_end - 1] == b'"'
            {
                // cleanly wrapped; scan the interior inline for stray quotes
                let a = raw_start + 1;
                let b = raw_end - 1;
                let mut j = a;
                let mut bad = false;
                while j + 8 <= b {
                    let w = u64::from_le_bytes(line[j..j + 8].try_into().unwrap());
                    if eq_mask(w, b'"') != 0 {
                        bad = true;
                        break;
                    }
                    j += 8;
                }
                if !bad {
                    while j < b {
                        if line[j] == b'"' {
                            bad = true;
                            break;
                        }
                        j += 1;
                    }
                }
                if bad {
                    return None;
                }
                *f = &line[a..b];
            } else {
                *f = unquote(f)?;
            }
        }
        raw_start = raw_end + 1;
    }
    Some(out)
}

#[inline(always)]
pub fn split2(line: &[u8]) -> Option<(&[u8], &[u8])> {
    let f = split_n::<2>(line)?;
    Some((f[0], f[1]))
}
