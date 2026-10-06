/// Generate a random frame mask.
#[inline]
pub fn generate_mask() -> [u8; 4] {
    rand::random()
}

/// Mask/unmask a frame.
#[inline]
pub fn apply_mask(buf: &mut [u8], mask: [u8; 4]) {
    apply_mask_fast32(buf, mask);
}

/// A safe unoptimized mask application.
#[inline]
fn apply_mask_fallback(buf: &mut [u8], mask: [u8; 4]) {
    for (i, byte) in buf.iter_mut().enumerate() {
        *byte ^= mask[i & 3];
    }
}

/// Faster version of `apply_mask()` which operates on 4-byte blocks.
#[inline]
pub fn apply_mask_fast32(buf: &mut [u8], mask: [u8; 4]) {
    let mask_u32 = u32::from_ne_bytes(mask);

    let (prefix, words, suffix) = unsafe { buf.align_to_mut::<u32>() };
    apply_mask_fallback(prefix, mask);
    let head = prefix.len() & 3;
    let mask_u32 = if head > 0 {
        if cfg!(target_endian = "big") {
            mask_u32.rotate_left(8 * head as u32)
        } else {
            mask_u32.rotate_right(8 * head as u32)
        }
    } else {
        mask_u32
    };
    for word in words.iter_mut() {
        *word ^= mask_u32;
    }
    apply_mask_fallback(suffix, mask_u32.to_ne_bytes());
}

/// Append a masked payload in one pass without modifying its shared source.
#[inline]
pub(crate) fn extend_masked(buf: &mut bytes::BytesMut, payload: &[u8], mask: [u8; 4]) {
    buf.reserve(payload.len());
    let old_len = buf.len();
    let word_mask = u64::from_ne_bytes([
        mask[0], mask[1], mask[2], mask[3], mask[0], mask[1], mask[2], mask[3],
    ]);
    {
        let mut source = payload.chunks_exact(8);
        let mut target = buf.spare_capacity_mut()[..payload.len()].chunks_exact_mut(8);
        for (src, dst) in source.by_ref().zip(target.by_ref()) {
            let word = u64::from_ne_bytes(src.try_into().unwrap()) ^ word_mask;
            // SAFETY: dst contains eight writable spare bytes. Unaligned writes are
            // supported, and all eight bytes are initialized before exposing them.
            unsafe { std::ptr::write_unaligned(dst.as_mut_ptr().cast::<u64>(), word) };
        }
        // Every complete block is a multiple of the four-byte mask period.
        for (index, (src, dst)) in source
            .remainder()
            .iter()
            .zip(target.into_remainder())
            .enumerate()
        {
            dst.write(*src ^ mask[index & 3]);
        }
    }
    // SAFETY: reserve ensured sufficient capacity, and the loops initialized
    // exactly payload.len() new bytes. Existing bytes were left untouched.
    unsafe { buf.set_len(old_len + payload.len()) };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masked_append_matches_reference_for_alignments_and_tails() {
        use bytes::BytesMut;
        for size in (0usize..=33).chain([
            63, 64, 65, 125, 126, 127, 255, 256, 257, 65535, 65536, 1048576,
        ]) {
            let payload: Vec<u8> = (0..size).map(|i| (i.wrapping_mul(17) + 13) as u8).collect();
            for mask in [[0; 4], [1, 2, 3, 4], [0x6d, 0xb6, 0xb2, 0x80]] {
                let mut reference = payload.clone();
                apply_mask_fallback(&mut reference, mask);
                for prefix in 0..16 {
                    let mut out = BytesMut::with_capacity(prefix + size);
                    out.extend_from_slice(&vec![0xa5; prefix]);
                    extend_masked(&mut out, &payload, mask);
                    assert_eq!(&out[..prefix], &vec![0xa5; prefix]);
                    assert_eq!(&out[prefix..], &reference);
                }
            }
        }
    }

    #[test]
    fn masked_append_preserves_source_sharing_the_allocation() {
        use bytes::BytesMut;
        let mut out = BytesMut::with_capacity(1024);
        out.extend_from_slice(&[0x5a; 256]);
        let payload = out.split_to(128).freeze();
        let prefix = out.to_vec();
        extend_masked(&mut out, &payload, [1, 2, 3, 4]);
        assert!(payload.iter().all(|&byte| byte == 0x5a));
        assert_eq!(&out[..128], &prefix);
        let mut reference = payload.to_vec();
        apply_mask_fallback(&mut reference, [1, 2, 3, 4]);
        assert_eq!(&out[128..], &reference);
    }

    #[test]
    fn test_apply_mask() {
        let mask = [0x6d, 0xb6, 0xb2, 0x80];
        let unmasked = [
            0xf3, 0x00, 0x01, 0x02, 0x03, 0x80, 0x81, 0x82, 0xff, 0xfe, 0x00, 0x17, 0x74, 0xf9,
            0x12, 0x03,
        ];

        for data_len in 0..=unmasked.len() {
            let unmasked = &unmasked[0..data_len];
            // Check masking with different alignment.
            for off in 0..=3 {
                if unmasked.len() < off {
                    continue;
                }
                let mut masked = unmasked.to_vec();
                apply_mask_fallback(&mut masked[off..], mask);

                let mut masked_fast = unmasked.to_vec();
                apply_mask_fast32(&mut masked_fast[off..], mask);

                assert_eq!(masked, masked_fast);
            }
        }
    }
}
