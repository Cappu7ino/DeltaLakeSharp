// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

use std::collections::BTreeMap;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use datafusion::logical_expr::{Expr, col, lit};
use deltalake::kernel::Add;

use super::super::helpers::{get_active_add_actions, has_reader_feature, table_protocol};
use super::super::request::{
    PartitionDescriptorMode, PartitionDescriptorPayload, PartitionPredicateKey, ReadCommand,
};
use crate::error::ServiceError;

const DEFAULT_PARTITION_MULTIPLIER: usize = 4;

#[derive(Debug, Clone)]
pub(super) struct PlannedPartition {
    pub(super) version: i64,
    pub(super) mode: PlannedPartitionMode,
}

#[derive(Debug, Clone)]
pub(super) enum PlannedPartitionMode {
    FileSubset { files: Vec<Add> },
    PartitionPredicate { keys: Vec<PartitionPredicateKey> },
}

#[derive(Debug, Clone)]
struct PartitionPlanningUnit {
    mode: PartitionPlanningUnitMode,
    total_size: i64,
}

#[derive(Debug, Clone)]
enum PartitionPlanningUnitMode {
    FileSubset { files: Vec<Add> },
    PartitionPredicate { key: PartitionPredicateKey },
}

enum CoalescingMode {
    FileSubset { files: Vec<Add> },
    PartitionPredicate { keys: Vec<PartitionPredicateKey> },
}

impl CoalescingMode {
    fn into_mode(self) -> PlannedPartitionMode {
        match self {
            CoalescingMode::FileSubset { files } => PlannedPartitionMode::FileSubset { files },
            CoalescingMode::PartitionPredicate { keys } => {
                PlannedPartitionMode::PartitionPredicate { keys }
            }
        }
    }
}

impl PlannedPartition {
    fn from_unit(version: i64, unit: PartitionPlanningUnit) -> Self {
        let mode = match unit.mode {
            PartitionPlanningUnitMode::FileSubset { files } => {
                PlannedPartitionMode::FileSubset { files }
            }
            PartitionPlanningUnitMode::PartitionPredicate { key } => {
                PlannedPartitionMode::PartitionPredicate { keys: vec![key] }
            }
        };

        Self { version, mode }
    }
}

pub(super) async fn plan_read_partitions(
    cmd: ReadCommand,
) -> Result<Vec<PlannedPartition>, ServiceError> {
    let (table, mut adds) = get_active_add_actions(
        &cmd.path,
        cmd.storage_account.as_deref(),
        cmd.sas_token.as_deref(),
        cmd.storage_options.as_ref(),
        cmd.version,
    )
    .await?;

    let partition_columns = table
        .snapshot()
        .map_err(ServiceError::Delta)?
        .metadata()
        .partition_columns()
        .to_vec();

    ensure_partition_planning_supported(&table, &adds, &partition_columns)?;

    if adds.is_empty() {
        return Ok(Vec::new());
    }

    adds.sort_by(|left, right| left.path.cmp(&right.path));

    let desired_partitions = choose_partition_count(adds.len());
    let mut units = build_planning_units(adds, &partition_columns);
    split_large_units(&mut units, desired_partitions);
    let table_version = table.version().ok_or_else(|| {
        ServiceError::Internal("expected loaded Delta table to expose a pinned version".to_string())
    })?;

    let planned = if units.len() > desired_partitions {
        coalesce_units(units, desired_partitions, table_version)
    } else {
        units.into_iter()
            .map(|unit| PlannedPartition::from_unit(table_version, unit))
            .collect()
    };

    Ok(planned)
}

pub(super) async fn resolve_partition_files(
    cmd: &ReadCommand,
    descriptor: &PartitionDescriptorPayload,
) -> Result<(i64, Vec<Add>), ServiceError> {
    let version = resolve_partition_token_version(cmd, descriptor.version)?;

    let PartitionDescriptorMode::FileSubset { file_paths } = &descriptor.mode else {
        return Err(ServiceError::InvalidRequest(
            "expected file-subset partition token".to_string(),
        ));
    };

    let (table, adds) = get_active_add_actions(
        &cmd.path,
        cmd.storage_account.as_deref(),
        cmd.sas_token.as_deref(),
        cmd.storage_options.as_ref(),
        Some(version),
    )
    .await?;

    ensure_partitioned_reads_supported(&table, &adds)?;

    let add_lookup = adds
        .into_iter()
        .map(|add| (add.path.clone(), add))
        .collect::<BTreeMap<_, _>>();

    let files = file_paths
        .iter()
        .map(|path| {
            add_lookup.get(path).cloned().ok_or_else(|| {
                ServiceError::InvalidRequest(format!(
                    "partition token references file '{path}' that is not active in table version {}",
                    version
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok((version, files))
}

pub(super) fn resolve_partition_token_version(
    cmd: &ReadCommand,
    token_version: i64,
) -> Result<i64, ServiceError> {
    if let Some(requested_version) = cmd.version
        && requested_version != token_version
    {
        return Err(ServiceError::InvalidRequest(format!(
            "partition token version {} does not match requested version {}",
            token_version, requested_version
        )));
    }

    Ok(token_version)
}

pub(super) fn ensure_partitioned_reads_supported(
    table: &deltalake::DeltaTable,
    adds: &[Add],
) -> Result<(), ServiceError> {
    if table_has_deletion_vectors(table, adds)? {
        return Err(ServiceError::InvalidRequest(
            "partitioned reads are not yet supported for Delta tables with deletion vectors"
                .to_string(),
        ));
    }

    Ok(())
}

fn ensure_partition_planning_supported(
    table: &deltalake::DeltaTable,
    adds: &[Add],
    partition_columns: &[String],
) -> Result<(), ServiceError> {
    if table_has_deletion_vectors(table, adds)? && partition_columns.is_empty() {
        return Err(ServiceError::InvalidRequest(
            "partitioned reads are not yet supported for Delta tables with deletion vectors"
                .to_string(),
        ));
    }

    Ok(())
}

fn table_has_deletion_vectors(
    table: &deltalake::DeltaTable,
    adds: &[Add],
) -> Result<bool, ServiceError> {
    let protocol = table_protocol(table)?;
    Ok(has_reader_feature(&protocol, "deletionVectors")
        || adds.iter().any(|add| add.deletion_vector.is_some()))
}

fn choose_partition_count(file_count: usize) -> usize {
    let available_parallelism = std::thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1);
    let desired = available_parallelism.saturating_mul(DEFAULT_PARTITION_MULTIPLIER);
    desired.clamp(1, file_count.max(1))
}

fn build_planning_units(adds: Vec<Add>, partition_columns: &[String]) -> Vec<PartitionPlanningUnit> {
    if partition_columns.is_empty() {
        return adds
            .into_iter()
            .map(|file| PartitionPlanningUnit {
                mode: PartitionPlanningUnitMode::FileSubset {
                    files: vec![file.clone()],
                },
                total_size: file.size,
            })
            .collect();
    }

    let mut groups = BTreeMap::<String, Vec<Add>>::new();
    for add in adds {
        let key = partition_key(&add, partition_columns);
        groups.entry(key).or_default().push(add);
    }

    groups
        .into_values()
        .map(|mut files| {
            files.sort_by(|left, right| left.path.cmp(&right.path));
            let total_size = files.iter().map(|file| file.size).sum();
            let key = PartitionPredicateKey {
                values: partition_columns
                    .iter()
                    .map(|column| {
                        (
                            column.clone(),
                            files[0].partition_values.get(column).cloned().unwrap_or(None),
                        )
                    })
                    .collect(),
            };
            PartitionPlanningUnit {
                mode: PartitionPlanningUnitMode::PartitionPredicate { key },
                total_size,
            }
        })
        .collect()
}

fn split_large_units(units: &mut Vec<PartitionPlanningUnit>, desired_partitions: usize) {
    while units.len() < desired_partitions {
        let Some((index, _)) = units
            .iter()
            .enumerate()
            .filter(|(_, unit)| match &unit.mode {
                PartitionPlanningUnitMode::FileSubset { files } => files.len() > 1,
                PartitionPlanningUnitMode::PartitionPredicate { .. } => false,
            })
            .max_by(|(left_index, left_unit), (right_index, right_unit)| {
                left_unit
                    .total_size
                    .cmp(&right_unit.total_size)
                    .then_with(|| right_index.cmp(left_index))
            })
        else {
            break;
        };

        let mut unit = units.remove(index);
        let PartitionPlanningUnitMode::FileSubset { files } = &mut unit.mode else {
            break;
        };
        let split_at = choose_split_index(files, unit.total_size);
        let right_files = files.split_off(split_at);
        let left_size = files.iter().map(|file| file.size).sum();
        let right_size = right_files.iter().map(|file| file.size).sum();

        units.insert(
            index,
            PartitionPlanningUnit {
                mode: PartitionPlanningUnitMode::FileSubset { files: right_files },
                total_size: right_size,
            },
        );
        units.insert(
            index,
            PartitionPlanningUnit {
                mode: unit.mode,
                total_size: left_size,
            },
        );
    }
}

fn choose_split_index(files: &[Add], total_size: i64) -> usize {
    let target_size = (total_size / 2).max(1);
    let mut current_size = 0_i64;
    for (index, file) in files.iter().enumerate().take(files.len().saturating_sub(1)) {
        current_size += file.size.max(1);
        if current_size >= target_size {
            return index + 1;
        }
    }

    files.len() / 2
}

fn coalesce_units(
    units: Vec<PartitionPlanningUnit>,
    target_partitions: usize,
    version: i64,
) -> Vec<PlannedPartition> {
    // Coalescing is only used after initial planning has already decided what the
    // atomic planning units are:
    // - non-partitioned tables use file-subset units
    // - Delta-partitioned tables use predicate-key units
    //
    // This function never splits a unit. It only groups adjacent units of the
    // same kind into fewer final partitions so we end up close to the requested
    // partition count.
    //
    // For PartitionPredicate mode, "coalescing" means storing multiple exact
    // partition keys in one token. Execution later turns those keys into an OR
    // predicate such as:
    //   (region = 'us') OR (region = 'eu')
    // rather than trying to merge the keys into a single broader predicate.
    let mut planned = Vec::with_capacity(target_partitions);
    let mut current_mode: Option<CoalescingMode> = None;
    let mut current_size = 0_i64;
    let mut remaining_size = units.iter().map(|unit| unit.total_size).sum::<i64>();
    let mut remaining_partitions = target_partitions;
    let total_units = units.len();

    for (index, unit) in units.into_iter().enumerate() {
        // Grow the current output partition by one unit.
        current_size += unit.total_size;
        current_mode = Some(match (current_mode.take(), unit.mode) {
            (None, PartitionPlanningUnitMode::FileSubset { files }) => {
                CoalescingMode::FileSubset { files }
            }
            (None, PartitionPlanningUnitMode::PartitionPredicate { key }) => {
                CoalescingMode::PartitionPredicate { keys: vec![key] }
            }
            (
                Some(CoalescingMode::FileSubset { mut files }),
                PartitionPlanningUnitMode::FileSubset { files: unit_files },
            ) => {
                files.extend(unit_files);
                CoalescingMode::FileSubset { files }
            }
            (
                Some(CoalescingMode::PartitionPredicate { mut keys }),
                PartitionPlanningUnitMode::PartitionPredicate { key },
            ) => {
                keys.push(key);
                CoalescingMode::PartitionPredicate { keys }
            }
            (Some(existing), incoming) => {
                // We only coalesce like-with-like. If the current accumulated
                // partition is file-subset based and the next unit is predicate
                // based (or vice versa), flush the current partition first and
                // start a new accumulator for the incoming unit.
                planned.push(PlannedPartition {
                    version,
                    mode: existing.into_mode(),
                });
                current_size = unit.total_size;
                match incoming {
                    PartitionPlanningUnitMode::FileSubset { files } => {
                        CoalescingMode::FileSubset { files }
                    }
                    PartitionPlanningUnitMode::PartitionPredicate { key } => {
                        CoalescingMode::PartitionPredicate { keys: vec![key] }
                    }
                }
            }
        });

        let units_left = total_units - index - 1;
        // If the number of remaining units exactly matches the number of
        // remaining output partitions, we must close here so every later unit
        // can occupy its own partition.
        let must_close = remaining_partitions > 1 && units_left == remaining_partitions - 1;
        // Otherwise, use a greedy target size based on the average remaining
        // bytes per remaining partition. Once the accumulator reaches or exceeds
        // that target, emit it and continue with a fresh partition.
        let target_size = div_ceil_i64(remaining_size, remaining_partitions as i64);
        let should_close = remaining_partitions > 1 && current_size >= target_size;

        if must_close || should_close {
            remaining_size -= current_size;
            remaining_partitions -= 1;
            planned.push(PlannedPartition {
                version,
                mode: current_mode.take().expect("coalescing mode").into_mode(),
            });
            current_size = 0;
        }
    }

    if let Some(current_mode) = current_mode {
        planned.push(PlannedPartition {
            version,
            mode: current_mode.into_mode(),
        });
    }

    planned
}

fn div_ceil_i64(value: i64, divisor: i64) -> i64 {
    (value + divisor - 1) / divisor
}

fn partition_key(add: &Add, partition_columns: &[String]) -> String {
    partition_columns
        .iter()
        .map(|column| {
            let value = add
                .partition_values
                .get(column)
                .and_then(|value| value.as_deref())
                .unwrap_or("__NULL__");
            format!("{column}={value}")
        })
        .collect::<Vec<_>>()
        .join("/")
}

pub(super) fn apply_partition_predicate_filter(
    df: datafusion::dataframe::DataFrame,
    keys: &[PartitionPredicateKey],
) -> datafusion::common::Result<datafusion::dataframe::DataFrame> {
    let mut combined: Option<Expr> = None;
    for key in keys {
        let mut key_expr: Option<Expr> = None;
        for (column, value) in &key.values {
            let expr = match value {
                Some(value) => col(column).eq(lit(value.clone())),
                None => col(column).is_null(),
            };

            key_expr = Some(match key_expr {
                Some(existing) => existing.and(expr),
                None => expr,
            });
        }

        let key_expr = key_expr.ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "partition predicate key must contain at least one column".to_string(),
            )
        })?;

        combined = Some(match combined {
            Some(existing) => existing.or(key_expr),
            None => key_expr,
        });
    }

    match combined {
        Some(expr) => df.filter(expr),
        None => Err(datafusion::error::DataFusionError::Execution(
            "partition predicate token must contain at least one key".to_string(),
        )),
    }
}

pub(super) fn encode_partition_token(
    descriptor: &PartitionDescriptorPayload,
) -> Result<String, ServiceError> {
    let bytes = serde_json::to_vec(descriptor).map_err(ServiceError::Json)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

pub(super) fn decode_partition_token(
    token: &str,
) -> Result<PartitionDescriptorPayload, ServiceError> {
    let bytes = URL_SAFE_NO_PAD.decode(token).map_err(|error| {
        ServiceError::InvalidRequest(format!("invalid partition token encoding: {error}"))
    })?;
    serde_json::from_slice(&bytes).map_err(ServiceError::Json)
}
