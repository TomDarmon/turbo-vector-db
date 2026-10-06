use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::keys::{now_rfc3339, sha256_hex};

pub(crate) const SHARD_NAMESPACE_DELIMITER: &str = "__tvs";
pub(crate) const SHARD_DEGRADED_REASON_DROPPED: &str = "dropped_shard";
pub(crate) const SHARD_DEGRADED_REASON_TIMEOUT: &str = "slow_shard_timeout";
pub(crate) const SHARD_DEGRADED_REASON_STALE: &str = "stale_generation_shard";
pub(crate) const SHARD_DEGRADED_REASON_ERROR: &str = "shard_error";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ShardAssignmentStrategy {
    HashIdV1,
}

impl Default for ShardAssignmentStrategy {
    fn default() -> Self {
        Self::HashIdV1
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ShardNodeState {
    Active,
    Draining,
    Offline,
}

impl Default for ShardNodeState {
    fn default() -> Self {
        Self::Active
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct ShardPlacementAssignment {
    pub(crate) shard_id: u32,
    pub(crate) node_id: String,
    #[serde(default)]
    pub(crate) pinned: bool,
    #[serde(default)]
    pub(crate) state: ShardNodeState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) min_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) simulated_delay_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ShardMigrationPhase {
    Planned,
    Syncing,
    Cutover,
    Complete,
    Aborted,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct ShardMigrationPlan {
    pub(crate) migration_id: String,
    pub(crate) phase: ShardMigrationPhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) source_node: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) target_node: Option<String>,
    #[serde(default)]
    pub(crate) shard_ids: Vec<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) min_visible_generation: Option<u64>,
    #[serde(default)]
    pub(crate) safety_checks: Vec<String>,
    pub(crate) updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct CollectionShardPlacement {
    pub(crate) collection: String,
    pub(crate) version: u64,
    #[serde(default)]
    pub(crate) strategy: ShardAssignmentStrategy,
    pub(crate) shard_count: u32,
    pub(crate) assignments: Vec<ShardPlacementAssignment>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) migration: Option<ShardMigrationPlan>,
    pub(crate) updated_at: String,
    pub(crate) updated_by: String,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub(crate) struct UpdateShardPlacementRequest {
    pub(crate) shard_count: u32,
    pub(crate) assignments: Vec<ShardPlacementAssignment>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) expected_version: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) migration: Option<ShardMigrationPlan>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub(crate) struct RebalanceShardPlacementRequest {
    pub(crate) target_nodes: Vec<String>,
    #[serde(default)]
    pub(crate) dry_run: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) shard_count: Option<u32>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct RebalanceShardPlacementResponse {
    pub(crate) applied: bool,
    pub(crate) safety_checks: Vec<String>,
    pub(crate) placement: CollectionShardPlacement,
}

pub(crate) fn shard_namespace(logical_namespace: &str, shard_id: u32) -> String {
    format!("{logical_namespace}{SHARD_NAMESPACE_DELIMITER}{shard_id:04}")
}

pub(crate) fn extract_shard_id(namespace: &str) -> Option<u32> {
    let (logical, suffix) = namespace.rsplit_once(SHARD_NAMESPACE_DELIMITER)?;
    if logical.is_empty() {
        return None;
    }
    if suffix.len() != 4 {
        return None;
    }
    suffix.parse::<u32>().ok()
}

pub(crate) fn shard_scope_label(namespace: &str) -> String {
    extract_shard_id(namespace)
        .map(|shard_id| format!("shard_{shard_id}"))
        .unwrap_or_else(|| "unsharded".to_string())
}

pub(crate) fn shard_namespaces(logical_namespace: &str, shard_count: usize) -> Vec<(u32, String)> {
    if shard_count <= 1 {
        return vec![(0, logical_namespace.to_string())];
    }
    (0..shard_count as u32)
        .map(|shard_id| (shard_id, shard_namespace(logical_namespace, shard_id)))
        .collect()
}

pub(crate) fn shard_for_vector_id(
    collection: &str,
    logical_namespace: &str,
    vector_id: &str,
    shard_count: usize,
) -> u32 {
    if shard_count <= 1 {
        return 0;
    }
    let hash_input = format!("{collection}\u{1f}{logical_namespace}\u{1f}{vector_id}");
    let digest = sha256_hex(hash_input.as_bytes());
    let prefix = digest.get(..16).unwrap_or("0");
    let numeric = u64::from_str_radix(prefix, 16).unwrap_or(0);
    (numeric % (shard_count as u64)) as u32
}

pub(crate) fn default_shard_placement(
    collection: &str,
    shard_count: usize,
    node_id: &str,
    updated_by: &str,
) -> CollectionShardPlacement {
    let shard_count = shard_count.max(1) as u32;
    let assignments = (0..shard_count)
        .map(|shard_id| ShardPlacementAssignment {
            shard_id,
            node_id: node_id.to_string(),
            pinned: true,
            state: ShardNodeState::Active,
            min_generation: None,
            simulated_delay_ms: None,
        })
        .collect();
    CollectionShardPlacement {
        collection: collection.to_string(),
        version: 1,
        strategy: ShardAssignmentStrategy::HashIdV1,
        shard_count,
        assignments,
        migration: None,
        updated_at: now_rfc3339(),
        updated_by: updated_by.to_string(),
    }
}

pub(crate) fn validate_placement(placement: &CollectionShardPlacement) -> Result<(), String> {
    if placement.collection.trim().is_empty() {
        return Err("placement.collection must not be empty".to_string());
    }
    if placement.shard_count == 0 {
        return Err("placement.shard_count must be > 0".to_string());
    }
    if placement.assignments.len() != placement.shard_count as usize {
        return Err(format!(
            "placement.assignments length ({}) must equal shard_count ({})",
            placement.assignments.len(),
            placement.shard_count
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    for assignment in &placement.assignments {
        if assignment.shard_id >= placement.shard_count {
            return Err(format!(
                "placement assignment shard_id {} must be < shard_count {}",
                assignment.shard_id, placement.shard_count
            ));
        }
        if assignment.node_id.trim().is_empty() {
            return Err(format!(
                "placement assignment for shard {} must include node_id",
                assignment.shard_id
            ));
        }
        if !seen.insert(assignment.shard_id) {
            return Err(format!(
                "placement assignment shard_id {} is duplicated",
                assignment.shard_id
            ));
        }
    }
    Ok(())
}

pub(crate) fn rebalance_assignments(
    shard_count: u32,
    target_nodes: &[String],
) -> Result<Vec<ShardPlacementAssignment>, String> {
    if shard_count == 0 {
        return Err("rebalance shard_count must be > 0".to_string());
    }
    if target_nodes.is_empty() {
        return Err("rebalance target_nodes must not be empty".to_string());
    }
    let mut normalized_nodes = target_nodes
        .iter()
        .map(|node| node.trim())
        .filter(|node| !node.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    normalized_nodes.sort();
    normalized_nodes.dedup();
    if normalized_nodes.is_empty() {
        return Err("rebalance target_nodes must contain at least one non-empty node".to_string());
    }

    let assignments = (0..shard_count)
        .map(|shard_id| {
            let node_index = (shard_id as usize) % normalized_nodes.len();
            ShardPlacementAssignment {
                shard_id,
                node_id: normalized_nodes[node_index].clone(),
                pinned: true,
                state: ShardNodeState::Active,
                min_generation: None,
                simulated_delay_ms: None,
            }
        })
        .collect();
    Ok(assignments)
}
