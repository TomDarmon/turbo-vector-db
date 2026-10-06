use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SegmentRef {
    pub segment_id: String,
    pub uri: String,
    pub row_count: u64,
    pub checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Manifest {
    pub generation: u64,
    pub collection: String,
    pub created_at: String,
    pub created_by: String,
    pub previous_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_applied_operation: Option<String>,
    pub namespace_partitions: BTreeMap<String, Vec<String>>,
    pub segment_refs: Vec<SegmentRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CurrentPointer {
    pub current_generation: u64,
    pub updated_at: String,
}
