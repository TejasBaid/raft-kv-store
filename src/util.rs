//! Small shared helpers.

/// 64-bit FNV-1a hash. Stable across runs and platforms (unlike `DefaultHasher`), which
/// matters for deterministic simulation traces.
pub fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Folds `value` into a running FNV-1a hash.
pub fn fnv1a_extend(h: u64, value: u64) -> u64 {
    let mut h = h;
    for b in value.to_le_bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[cfg(test)]
mod tests {
    #[test]
    fn fnv_known_vectors() {
        assert_eq!(super::fnv1a(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(super::fnv1a(b"a"), 0xaf63_dc4c_8601_ec8c);
    }
}
