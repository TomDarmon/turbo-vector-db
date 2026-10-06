use std::collections::BTreeMap;

use crate::error::ApiError;

const BLOCK_PACK_MAGIC: &[u8; 4] = b"TVPK";
const BLOCK_PACK_VERSION: u8 = 1;

const HEADER_LEN: usize = 4  // magic
    + 1 // version
    + 1 // flags
    + 2 // reserved
    + 4 // block count
    + 4 // directory length
    + 4; // payload checksum

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlockPackEntry {
    pub(crate) offset: u32,
    pub(crate) len: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct DecodedBlockPack {
    payload: Vec<u8>,
    directory: BTreeMap<String, BlockPackEntry>,
}

impl DecodedBlockPack {
    pub(crate) fn entry(&self, block_id: &str) -> Option<&BlockPackEntry> {
        self.directory.get(block_id)
    }

    pub(crate) fn block_slice(
        &self,
        block_id: &str,
        expected_offset: u32,
        expected_len: u32,
    ) -> Result<&[u8], ApiError> {
        let entry = self.directory.get(block_id).ok_or_else(|| {
            ApiError::store_unavailable(format!("missing block_id '{block_id}' in pack directory"))
        })?;
        if entry.offset != expected_offset || entry.len != expected_len {
            return Err(ApiError::store_unavailable(format!(
                "block '{block_id}' directory mismatch (offset/len)"
            )));
        }
        let start = expected_offset as usize;
        let end = start.saturating_add(expected_len as usize);
        self.payload.get(start..end).ok_or_else(|| {
            ApiError::store_unavailable(format!("block '{block_id}' exceeds pack bounds"))
        })
    }
}

pub(crate) fn encode_block_pack(blocks: &[(String, Vec<u8>)]) -> Result<Vec<u8>, ApiError> {
    if blocks.is_empty() {
        return Err(ApiError::internal("cannot encode empty block pack"));
    }
    let block_count = u32::try_from(blocks.len())
        .map_err(|_| ApiError::internal("block pack count exceeds u32"))?;

    let mut directory = Vec::new();
    let mut payload = Vec::new();
    for (block_id, block_bytes) in blocks {
        if block_id.is_empty() {
            return Err(ApiError::internal("block pack block_id cannot be empty"));
        }
        if block_bytes.is_empty() {
            return Err(ApiError::internal(
                "block pack cannot contain empty block payload",
            ));
        }
        let block_id_bytes = block_id.as_bytes();
        let block_id_len = u16::try_from(block_id_bytes.len())
            .map_err(|_| ApiError::internal("block_id length exceeds u16"))?;
        let offset = u32::try_from(payload.len())
            .map_err(|_| ApiError::internal("block pack payload offset exceeds u32"))?;
        let len = u32::try_from(block_bytes.len())
            .map_err(|_| ApiError::internal("block pack payload length exceeds u32"))?;
        directory.extend_from_slice(&block_id_len.to_le_bytes());
        directory.extend_from_slice(block_id_bytes);
        directory.extend_from_slice(&offset.to_le_bytes());
        directory.extend_from_slice(&len.to_le_bytes());
        payload.extend_from_slice(block_bytes);
    }
    let directory_len = u32::try_from(directory.len())
        .map_err(|_| ApiError::internal("block pack directory length exceeds u32"))?;
    let payload_checksum = checksum32(&payload);

    let mut out = Vec::with_capacity(
        HEADER_LEN
            .saturating_add(directory.len())
            .saturating_add(payload.len()),
    );
    out.extend_from_slice(BLOCK_PACK_MAGIC);
    out.push(BLOCK_PACK_VERSION);
    out.push(0_u8);
    out.extend_from_slice(&0_u16.to_le_bytes());
    out.extend_from_slice(&block_count.to_le_bytes());
    out.extend_from_slice(&directory_len.to_le_bytes());
    out.extend_from_slice(&payload_checksum.to_le_bytes());
    out.extend_from_slice(&directory);
    out.extend_from_slice(&payload);
    Ok(out)
}

pub(crate) fn decode_block_pack(raw: &[u8]) -> Result<DecodedBlockPack, ApiError> {
    if raw.len() < HEADER_LEN {
        return Err(ApiError::internal("block pack payload is truncated"));
    }
    if &raw[..4] != BLOCK_PACK_MAGIC {
        return Err(ApiError::internal("invalid block pack magic"));
    }
    if raw[4] != BLOCK_PACK_VERSION {
        return Err(ApiError::internal(format!(
            "unsupported block pack version {}",
            raw[4]
        )));
    }
    if raw[5] != 0 || raw[6] != 0 || raw[7] != 0 {
        return Err(ApiError::internal(
            "block pack header contains unsupported flags",
        ));
    }
    let block_count = u32::from_le_bytes(
        raw[8..12]
            .try_into()
            .map_err(|_| ApiError::internal("invalid block_count bytes"))?,
    ) as usize;
    if block_count == 0 {
        return Err(ApiError::internal(
            "block pack payload must contain at least one block",
        ));
    }
    let directory_len = u32::from_le_bytes(
        raw[12..16]
            .try_into()
            .map_err(|_| ApiError::internal("invalid directory_len bytes"))?,
    ) as usize;
    let payload_checksum = u32::from_le_bytes(
        raw[16..20]
            .try_into()
            .map_err(|_| ApiError::internal("invalid payload checksum bytes"))?,
    );
    let directory_end = HEADER_LEN.saturating_add(directory_len);
    if directory_end > raw.len() {
        return Err(ApiError::internal(
            "block pack directory length exceeds payload",
        ));
    }
    let directory_bytes = &raw[HEADER_LEN..directory_end];
    let payload = &raw[directory_end..];
    if checksum32(payload) != payload_checksum {
        return Err(ApiError::internal("block pack payload checksum mismatch"));
    }

    let mut directory = BTreeMap::new();
    let mut cursor = 0usize;
    for _ in 0..block_count {
        let block_id_len = decode_u16(directory_bytes, &mut cursor)? as usize;
        if block_id_len == 0 {
            return Err(ApiError::internal(
                "block pack directory contains empty block_id",
            ));
        }
        let id_end = cursor.saturating_add(block_id_len);
        if id_end > directory_bytes.len() {
            return Err(ApiError::internal(
                "block pack directory block_id is truncated",
            ));
        }
        let block_id = std::str::from_utf8(&directory_bytes[cursor..id_end])
            .map_err(|_| ApiError::internal("block pack directory block_id is not UTF-8"))?
            .to_string();
        cursor = id_end;
        let offset = decode_u32(directory_bytes, &mut cursor)?;
        let len = decode_u32(directory_bytes, &mut cursor)?;
        if directory
            .insert(block_id.clone(), BlockPackEntry { offset, len })
            .is_some()
        {
            return Err(ApiError::internal(format!(
                "duplicate block_id '{block_id}' in block pack directory"
            )));
        }
    }
    if cursor != directory_bytes.len() {
        return Err(ApiError::internal(
            "block pack directory contains trailing bytes",
        ));
    }

    let mut ranges = directory
        .iter()
        .map(|(block_id, entry)| {
            let start = entry.offset as usize;
            let end = start.saturating_add(entry.len as usize);
            (start, end, block_id.clone())
        })
        .collect::<Vec<_>>();
    ranges.sort_by_key(|(start, _, _)| *start);
    let mut previous_end = 0usize;
    for (start, end, block_id) in ranges {
        if end > payload.len() {
            return Err(ApiError::internal(format!(
                "block '{block_id}' exceeds block pack bounds"
            )));
        }
        if start < previous_end {
            return Err(ApiError::internal(format!(
                "block '{block_id}' overlaps previous block payload"
            )));
        }
        previous_end = end;
    }

    Ok(DecodedBlockPack {
        payload: payload.to_vec(),
        directory,
    })
}

fn checksum32(bytes: &[u8]) -> u32 {
    let mut hash = 0x811c9dc5_u32;
    for byte in bytes {
        hash ^= *byte as u32;
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}

fn decode_u16(raw: &[u8], cursor: &mut usize) -> Result<u16, ApiError> {
    let end = cursor.saturating_add(2);
    if end > raw.len() {
        return Err(ApiError::internal(
            "truncated u16 while decoding block pack",
        ));
    }
    let value = u16::from_le_bytes(
        raw[*cursor..end]
            .try_into()
            .map_err(|_| ApiError::internal("invalid u16 bytes in block pack"))?,
    );
    *cursor = end;
    Ok(value)
}

fn decode_u32(raw: &[u8], cursor: &mut usize) -> Result<u32, ApiError> {
    let end = cursor.saturating_add(4);
    if end > raw.len() {
        return Err(ApiError::internal(
            "truncated u32 while decoding block pack",
        ));
    }
    let value = u32::from_le_bytes(
        raw[*cursor..end]
            .try_into()
            .map_err(|_| ApiError::internal("invalid u32 bytes in block pack"))?,
    );
    *cursor = end;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::{decode_block_pack, encode_block_pack};

    #[test]
    fn block_pack_roundtrip_preserves_directory_entries() {
        let pack = encode_block_pack(&[
            ("b0".to_string(), vec![1, 2, 3]),
            ("b1".to_string(), vec![4, 5]),
        ])
        .expect("encode");
        let decoded = decode_block_pack(&pack).expect("decode");
        let b0 = decoded
            .entry("b0")
            .expect("entry b0 should be present")
            .clone();
        assert_eq!(
            decoded.block_slice("b0", b0.offset, b0.len).expect("slice"),
            &[1, 2, 3]
        );
        let b1 = decoded
            .entry("b1")
            .expect("entry b1 should be present")
            .clone();
        assert_eq!(
            decoded.block_slice("b1", b1.offset, b1.len).expect("slice"),
            &[4, 5]
        );
    }

    #[test]
    fn block_pack_detects_checksum_corruption() {
        let mut pack = encode_block_pack(&[("b0".to_string(), vec![1, 2, 3])]).expect("encode");
        let last = pack.len().saturating_sub(1);
        pack[last] ^= 0xFF;
        let decoded = decode_block_pack(&pack);
        assert!(decoded.is_err(), "corrupted payload must fail checksum");
    }
}
