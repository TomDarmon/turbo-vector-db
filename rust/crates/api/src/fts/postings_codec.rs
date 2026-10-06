use crate::error::ApiError;

const POSTINGS_BLOCK_MAGIC: &[u8; 4] = b"TVPB";
const POSTINGS_BLOCK_VERSION: u8 = 3;
const FRAME_COUNT: usize = 4;
const FRAME_ENTRY_LEN: usize = 8; // offset u32 + len u32
const FRAME_INDEX_DOC_DELTAS: usize = 0;
const FRAME_INDEX_TFS: usize = 1;
const FRAME_INDEX_DOC_LENS: usize = 2;
const FRAME_INDEX_FLAGS: usize = 3;

const HEADER_LEN: usize = 4  // magic
    + 1 // version
    + 1 // flags
    + 2 // reserved
    + 4 // posting_count
    + 8 // doc_id_min
    + 8 // doc_id_max
    + 4 // max_term_score
    + 2 // frame count
    + 2 // frame entry len
    + FRAME_COUNT * FRAME_ENTRY_LEN
    + 4; // payload checksum

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Posting {
    pub(crate) doc_id: u64,
    pub(crate) tf: u16,
    pub(crate) doc_len: u16,
    pub(crate) flags: u16,
}

#[derive(Debug, Clone)]
pub(crate) struct DecodedPostingsBlock {
    pub(crate) postings: Vec<Posting>,
    pub(crate) doc_id_min: u64,
    pub(crate) doc_id_max: u64,
    pub(crate) max_term_score: f32,
}

pub(crate) fn encode_postings_block(
    postings: &[Posting],
    max_term_score: f32,
) -> Result<Vec<u8>, ApiError> {
    if postings.is_empty() {
        return Err(ApiError::internal(
            "cannot encode empty postings block payload",
        ));
    }
    if !max_term_score.is_finite() {
        return Err(ApiError::internal(
            "postings block max_term_score must be finite",
        ));
    }

    validate_postings_are_sorted(postings)?;

    let doc_id_min = postings.first().map(|posting| posting.doc_id).unwrap_or(0);
    let doc_id_max = postings.last().map(|posting| posting.doc_id).unwrap_or(0);
    let posting_count = u32::try_from(postings.len())
        .map_err(|_| ApiError::internal("postings block posting_count exceeds u32"))?;

    let mut doc_delta_frame = Vec::with_capacity(postings.len());
    let mut tf_frame = Vec::with_capacity(postings.len());
    let mut doc_len_frame = Vec::with_capacity(postings.len());
    let mut flags_frame = Vec::with_capacity(postings.len());
    let mut previous_doc_id = 0_u64;
    for posting in postings {
        let delta = posting
            .doc_id
            .checked_sub(previous_doc_id)
            .ok_or_else(|| ApiError::internal("posting doc_id delta underflow"))?;
        encode_varint_u64(delta, &mut doc_delta_frame);
        encode_varint_u16(posting.tf, &mut tf_frame);
        encode_varint_u16(posting.doc_len, &mut doc_len_frame);
        encode_varint_u16(posting.flags, &mut flags_frame);
        previous_doc_id = posting.doc_id;
    }
    let frames = [doc_delta_frame, tf_frame, doc_len_frame, flags_frame];
    let payload_len = frames
        .iter()
        .try_fold(0_usize, |sum, frame| sum.checked_add(frame.len()))
        .ok_or_else(|| ApiError::internal("postings block payload length overflow"))?;
    let mut payload = Vec::with_capacity(payload_len);
    let mut frame_offsets = [0_u32; FRAME_COUNT];
    let mut frame_lengths = [0_u32; FRAME_COUNT];
    for (index, frame) in frames.iter().enumerate() {
        let offset = u32::try_from(payload.len())
            .map_err(|_| ApiError::internal("postings frame offset exceeds u32"))?;
        let length = u32::try_from(frame.len())
            .map_err(|_| ApiError::internal("postings frame length exceeds u32"))?;
        frame_offsets[index] = offset;
        frame_lengths[index] = length;
        payload.extend_from_slice(frame);
    }
    let payload_checksum = checksum32(&payload);

    let mut out = Vec::with_capacity(HEADER_LEN.saturating_add(payload.len()));
    out.extend_from_slice(POSTINGS_BLOCK_MAGIC);
    out.push(POSTINGS_BLOCK_VERSION);
    out.push(0_u8);
    out.extend_from_slice(&0_u16.to_le_bytes());
    out.extend_from_slice(&posting_count.to_le_bytes());
    out.extend_from_slice(&doc_id_min.to_le_bytes());
    out.extend_from_slice(&doc_id_max.to_le_bytes());
    out.extend_from_slice(&max_term_score.to_le_bytes());
    out.extend_from_slice(&(FRAME_COUNT as u16).to_le_bytes());
    out.extend_from_slice(&(FRAME_ENTRY_LEN as u16).to_le_bytes());
    for index in 0..FRAME_COUNT {
        out.extend_from_slice(&frame_offsets[index].to_le_bytes());
        out.extend_from_slice(&frame_lengths[index].to_le_bytes());
    }
    out.extend_from_slice(&payload_checksum.to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

pub(crate) fn decode_postings_block(raw: &[u8]) -> Result<DecodedPostingsBlock, ApiError> {
    if raw.len() < HEADER_LEN {
        return Err(ApiError::internal("postings block payload is truncated"));
    }
    let magic = &raw[..4];
    if magic != POSTINGS_BLOCK_MAGIC {
        return Err(ApiError::internal("invalid postings block magic"));
    }
    if raw[4] != POSTINGS_BLOCK_VERSION {
        return Err(ApiError::internal(format!(
            "unsupported postings block version {}",
            raw[4]
        )));
    }
    if raw[5] != 0 || raw[6] != 0 || raw[7] != 0 {
        return Err(ApiError::internal(
            "postings block header contains unsupported flags",
        ));
    }

    let posting_count = u32::from_le_bytes(
        raw[8..12]
            .try_into()
            .map_err(|_| ApiError::internal("invalid posting_count bytes"))?,
    ) as usize;
    if posting_count == 0 {
        return Err(ApiError::internal(
            "postings block payload must contain at least one posting",
        ));
    }
    let doc_id_min = u64::from_le_bytes(
        raw[12..20]
            .try_into()
            .map_err(|_| ApiError::internal("invalid doc_id_min bytes"))?,
    );
    let doc_id_max = u64::from_le_bytes(
        raw[20..28]
            .try_into()
            .map_err(|_| ApiError::internal("invalid doc_id_max bytes"))?,
    );
    let max_term_score = f32::from_le_bytes(
        raw[28..32]
            .try_into()
            .map_err(|_| ApiError::internal("invalid max_term_score bytes"))?,
    );
    if !max_term_score.is_finite() {
        return Err(ApiError::internal(
            "postings block max_term_score is not finite",
        ));
    }
    let frame_count = u16::from_le_bytes(
        raw[32..34]
            .try_into()
            .map_err(|_| ApiError::internal("invalid frame_count bytes"))?,
    ) as usize;
    if frame_count != FRAME_COUNT {
        return Err(ApiError::internal(format!(
            "unsupported postings frame count {}",
            frame_count
        )));
    }
    let frame_entry_len = u16::from_le_bytes(
        raw[34..36]
            .try_into()
            .map_err(|_| ApiError::internal("invalid frame_entry_len bytes"))?,
    ) as usize;
    if frame_entry_len != FRAME_ENTRY_LEN {
        return Err(ApiError::internal(format!(
            "unsupported postings frame entry len {}",
            frame_entry_len
        )));
    }
    let mut frame_offsets = [0_u32; FRAME_COUNT];
    let mut frame_lengths = [0_u32; FRAME_COUNT];
    let mut cursor = 36usize;
    for index in 0..FRAME_COUNT {
        let next = cursor.saturating_add(FRAME_ENTRY_LEN);
        if next > raw.len() {
            return Err(ApiError::internal("postings frame directory is truncated"));
        }
        frame_offsets[index] = u32::from_le_bytes(
            raw[cursor..cursor + 4]
                .try_into()
                .map_err(|_| ApiError::internal("invalid frame offset bytes"))?,
        );
        frame_lengths[index] = u32::from_le_bytes(
            raw[cursor + 4..cursor + 8]
                .try_into()
                .map_err(|_| ApiError::internal("invalid frame length bytes"))?,
        );
        cursor = next;
    }
    let expected_checksum = u32::from_le_bytes(
        raw[68..72]
            .try_into()
            .map_err(|_| ApiError::internal("invalid payload checksum bytes"))?,
    );
    let payload = &raw[HEADER_LEN..];
    if checksum32(payload) != expected_checksum {
        return Err(ApiError::internal(
            "postings block payload checksum mismatch",
        ));
    }
    validate_frame_ranges(payload.len(), &frame_offsets, &frame_lengths)?;

    let doc_delta_frame = frame_slice(
        payload,
        frame_offsets,
        frame_lengths,
        FRAME_INDEX_DOC_DELTAS,
    )?;
    let tf_frame = frame_slice(payload, frame_offsets, frame_lengths, FRAME_INDEX_TFS)?;
    let doc_len_frame = frame_slice(payload, frame_offsets, frame_lengths, FRAME_INDEX_DOC_LENS)?;
    let flags_frame = frame_slice(payload, frame_offsets, frame_lengths, FRAME_INDEX_FLAGS)?;

    let mut postings = Vec::with_capacity(posting_count);
    let mut delta_cursor = 0usize;
    let mut tf_cursor = 0usize;
    let mut doc_len_cursor = 0usize;
    let mut flags_cursor = 0usize;
    let mut previous_doc_id = 0_u64;
    for _ in 0..posting_count {
        let delta = decode_varint_u64(doc_delta_frame, &mut delta_cursor)?;
        let tf = decode_varint_u16(tf_frame, &mut tf_cursor)?;
        let doc_len = decode_varint_u16(doc_len_frame, &mut doc_len_cursor)?;
        let flags = decode_varint_u16(flags_frame, &mut flags_cursor)?;
        let doc_id = previous_doc_id
            .checked_add(delta)
            .ok_or_else(|| ApiError::internal("posting doc_id overflow while decoding"))?;
        postings.push(Posting {
            doc_id,
            tf,
            doc_len,
            flags,
        });
        previous_doc_id = doc_id;
    }
    validate_postings_are_sorted(&postings)?;
    let first_doc_id = postings.first().map(|posting| posting.doc_id).unwrap_or(0);
    let last_doc_id = postings.last().map(|posting| posting.doc_id).unwrap_or(0);
    if first_doc_id != doc_id_min || last_doc_id != doc_id_max {
        return Err(ApiError::internal(
            "postings block doc_id range header mismatch",
        ));
    }
    if delta_cursor != doc_delta_frame.len()
        || tf_cursor != tf_frame.len()
        || doc_len_cursor != doc_len_frame.len()
        || flags_cursor != flags_frame.len()
    {
        return Err(ApiError::internal(
            "postings frame decode did not consume full frame payload",
        ));
    }

    Ok(DecodedPostingsBlock {
        postings,
        doc_id_min,
        doc_id_max,
        max_term_score,
    })
}

pub(crate) fn estimate_max_term_score(postings: &[Posting]) -> f32 {
    postings.iter().fold(0.0_f32, |max_score, posting| {
        let denom = if posting.doc_len == 0 {
            1.0_f32
        } else {
            posting.doc_len as f32
        };
        let score = posting.tf as f32 / denom;
        max_score.max(score)
    })
}

fn validate_postings_are_sorted(postings: &[Posting]) -> Result<(), ApiError> {
    for window in postings.windows(2) {
        if window[0].doc_id >= window[1].doc_id {
            return Err(ApiError::internal(
                "postings must be strictly sorted by doc_id",
            ));
        }
    }
    Ok(())
}

fn checksum32(bytes: &[u8]) -> u32 {
    // FNV-1a checksum keeps validation lightweight and deterministic.
    let mut hash = 0x811c9dc5_u32;
    for byte in bytes {
        hash ^= *byte as u32;
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}

fn frame_slice<'a>(
    payload: &'a [u8],
    offsets: [u32; FRAME_COUNT],
    lengths: [u32; FRAME_COUNT],
    frame_index: usize,
) -> Result<&'a [u8], ApiError> {
    let offset = offsets
        .get(frame_index)
        .copied()
        .ok_or_else(|| ApiError::internal("frame index out of bounds"))? as usize;
    let len = lengths
        .get(frame_index)
        .copied()
        .ok_or_else(|| ApiError::internal("frame index out of bounds"))? as usize;
    let end = offset.saturating_add(len);
    if end > payload.len() {
        return Err(ApiError::internal("postings frame exceeds payload length"));
    }
    Ok(&payload[offset..end])
}

fn validate_frame_ranges(
    payload_len: usize,
    offsets: &[u32; FRAME_COUNT],
    lengths: &[u32; FRAME_COUNT],
) -> Result<(), ApiError> {
    let mut ranges = Vec::with_capacity(FRAME_COUNT);
    for index in 0..FRAME_COUNT {
        let start = offsets[index] as usize;
        let end = start.saturating_add(lengths[index] as usize);
        if end > payload_len {
            return Err(ApiError::internal(format!(
                "postings frame {index} exceeds payload bounds"
            )));
        }
        ranges.push((start, end, index));
    }
    ranges.sort_by_key(|(start, _, _)| *start);
    let mut previous_end = 0usize;
    for (start, end, index) in ranges {
        if start < previous_end {
            return Err(ApiError::internal(format!(
                "postings frame {index} overlaps prior frame"
            )));
        }
        previous_end = end;
    }
    if previous_end != payload_len {
        return Err(ApiError::internal(
            "postings frame directory does not cover full payload",
        ));
    }
    Ok(())
}

fn encode_varint_u16(value: u16, out: &mut Vec<u8>) {
    encode_varint_u64(value as u64, out);
}

fn encode_varint_u64(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8 & 0x7F) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn decode_varint_u16(raw: &[u8], cursor: &mut usize) -> Result<u16, ApiError> {
    let value = decode_varint_u64(raw, cursor)?;
    if value > u16::MAX as u64 {
        return Err(ApiError::internal(
            "decoded u16 varint exceeded numeric bounds",
        ));
    }
    Ok(value as u16)
}

fn decode_varint_u64(raw: &[u8], cursor: &mut usize) -> Result<u64, ApiError> {
    let mut shift = 0_u32;
    let mut value = 0_u64;
    while shift <= 63 {
        let byte = *raw
            .get(*cursor)
            .ok_or_else(|| ApiError::internal("truncated varint while decoding postings"))?;
        *cursor = cursor.saturating_add(1);
        value |= ((byte & 0x7F) as u64) << shift;
        if (byte & 0x80) == 0 {
            return Ok(value);
        }
        shift = shift.saturating_add(7);
    }
    Err(ApiError::internal("varint exceeded supported width"))
}

#[cfg(test)]
mod tests {
    use super::{decode_postings_block, encode_postings_block, estimate_max_term_score, Posting};

    #[test]
    fn postings_codec_roundtrip_preserves_content_and_metadata() {
        let postings = vec![
            Posting {
                doc_id: 7,
                tf: 2,
                doc_len: 10,
                flags: 0,
            },
            Posting {
                doc_id: 12,
                tf: 5,
                doc_len: 20,
                flags: 1,
            },
            Posting {
                doc_id: 98,
                tf: 1,
                doc_len: 9,
                flags: 3,
            },
        ];
        let max_term_score = estimate_max_term_score(&postings);
        let encoded =
            encode_postings_block(&postings, max_term_score).expect("postings should encode");
        let decoded = decode_postings_block(&encoded).expect("postings should decode");

        assert_eq!(decoded.postings, postings);
        assert_eq!(decoded.doc_id_min, 7);
        assert_eq!(decoded.doc_id_max, 98);
        assert!((decoded.max_term_score - max_term_score).abs() < 1e-6);
    }

    #[test]
    fn postings_codec_rejects_unsorted_input() {
        let unsorted = vec![
            Posting {
                doc_id: 10,
                tf: 1,
                doc_len: 5,
                flags: 0,
            },
            Posting {
                doc_id: 9,
                tf: 1,
                doc_len: 5,
                flags: 0,
            },
        ];
        let encoded = encode_postings_block(&unsorted, 1.0);
        assert!(encoded.is_err(), "unsorted postings must be rejected");
    }

    #[test]
    fn postings_codec_detects_payload_corruption() {
        let postings = vec![
            Posting {
                doc_id: 100,
                tf: 1,
                doc_len: 5,
                flags: 0,
            },
            Posting {
                doc_id: 120,
                tf: 3,
                doc_len: 7,
                flags: 0,
            },
        ];
        let mut encoded = encode_postings_block(&postings, 0.75).expect("encode");
        let last_index = encoded.len().saturating_sub(1);
        encoded[last_index] ^= 0xFF;
        let decoded = decode_postings_block(&encoded);
        assert!(decoded.is_err(), "corrupted payload should fail checksum");
    }

    #[test]
    fn postings_codec_is_deterministic_for_identical_input() {
        let postings = vec![
            Posting {
                doc_id: 3,
                tf: 1,
                doc_len: 5,
                flags: 0,
            },
            Posting {
                doc_id: 6,
                tf: 2,
                doc_len: 7,
                flags: 1,
            },
            Posting {
                doc_id: 700,
                tf: 1,
                doc_len: 9,
                flags: 0,
            },
        ];
        let first = encode_postings_block(&postings, estimate_max_term_score(&postings))
            .expect("encode first");
        let second = encode_postings_block(&postings, estimate_max_term_score(&postings))
            .expect("encode second");
        assert_eq!(first, second, "binary encoding must be deterministic");
    }

    #[test]
    fn postings_codec_rejects_truncated_frame_payload() {
        let postings = vec![
            Posting {
                doc_id: 10,
                tf: 4,
                doc_len: 12,
                flags: 0,
            },
            Posting {
                doc_id: 11,
                tf: 2,
                doc_len: 4,
                flags: 0,
            },
        ];
        let mut encoded =
            encode_postings_block(&postings, estimate_max_term_score(&postings)).expect("encode");
        let _ = encoded.pop();
        let decoded = decode_postings_block(&encoded);
        assert!(decoded.is_err(), "truncated frame payload must fail decode");
    }
}
