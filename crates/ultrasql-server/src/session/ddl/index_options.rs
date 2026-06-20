//! Access-method option parsing helpers for `CREATE INDEX`
//! (HNSW / IVFFlat / opclass / index options). Part of the
//! `session::ddl` module split.

use ultrasql_core::DataType;
use ultrasql_planner::{LogicalIndexMethod, LogicalIndexOption};
use ultrasql_storage::access_method::{AnnPayloadKind, HnswMetric};

use crate::error::ServerError;

pub(super) fn hnsw_metric_for_opclass(opclass: Option<&str>) -> Result<HnswMetric, ServerError> {
    match opclass.unwrap_or("vector_l2_ops") {
        "vector_l2_ops" => Ok(HnswMetric::L2),
        "vector_cosine_ops" => Ok(HnswMetric::Cosine),
        "vector_ip_ops" => Ok(HnswMetric::NegativeInnerProduct),
        "vector_l1_ops" => Ok(HnswMetric::L1),
        other => Err(ServerError::ddl(format!(
            "CREATE INDEX USING hnsw: unsupported vector opclass {other}"
        ))),
    }
}

pub(super) fn logical_index_method_name(method: LogicalIndexMethod) -> &'static str {
    match method {
        LogicalIndexMethod::Btree => "btree",
        LogicalIndexMethod::Hash => "hash",
        LogicalIndexMethod::Gin => "gin",
        LogicalIndexMethod::Gist => "gist",
        LogicalIndexMethod::Brin => "brin",
        LogicalIndexMethod::Hnsw => "hnsw",
        LogicalIndexMethod::IvfFlat => "ivfflat",
        LogicalIndexMethod::Aggregating => "aggregating",
    }
}

pub(super) fn index_options_as_pairs(options: &[LogicalIndexOption]) -> Vec<(String, String)> {
    options
        .iter()
        .map(|option| (option.name.clone(), option.value.clone()))
        .collect()
}
pub(super) fn ann_dims_and_default_payload(
    context: &str,
    data_type: &DataType,
) -> Result<(u32, AnnPayloadKind), ServerError> {
    match data_type {
        DataType::Vector { dims: Some(dims) } => Ok((*dims, AnnPayloadKind::F32)),
        DataType::HalfVec { dims: Some(dims) } => Ok((*dims, AnnPayloadKind::Bf16)),
        other => Err(ServerError::ddl(format!(
            "{context} requires vector(n) or halfvec(n), got {other}"
        ))),
    }
}

pub(super) fn hnsw_payload_option(
    options: &[LogicalIndexOption],
) -> Result<Option<AnnPayloadKind>, ServerError> {
    let mut payload = None;
    for option in options {
        if option.name != "payload" {
            return Err(ServerError::ddl(format!(
                "CREATE INDEX USING hnsw: unsupported option {}",
                option.name
            )));
        }
        payload = Some(ann_payload_kind_from_value(
            "CREATE INDEX USING hnsw",
            &option.value,
        )?);
    }
    Ok(payload)
}

fn ann_payload_kind_from_value(context: &str, value: &str) -> Result<AnnPayloadKind, ServerError> {
    match value.to_ascii_lowercase().as_str() {
        "f32" | "float32" => Ok(AnnPayloadKind::F32),
        "bf16" | "bfloat16" => Ok(AnnPayloadKind::Bf16),
        "int8" | "i8" => Ok(AnnPayloadKind::Int8),
        other => Err(ServerError::ddl(format!(
            "{context}: unsupported payload {other}; expected f32, bf16, or int8"
        ))),
    }
}

pub(super) fn ivfflat_options(
    options: &[LogicalIndexOption],
) -> Result<(usize, usize, Option<AnnPayloadKind>), ServerError> {
    let mut lists = 100_usize;
    let mut probes = 1_usize;
    let mut payload = None;
    for option in options {
        match option.name.as_str() {
            "lists" => lists = parse_positive_ivfflat_option(option)?,
            "probes" => probes = parse_positive_ivfflat_option(option)?,
            "payload" => {
                payload = Some(ann_payload_kind_from_value(
                    "CREATE INDEX USING ivfflat",
                    &option.value,
                )?);
            }
            other => {
                return Err(ServerError::ddl(format!(
                    "CREATE INDEX USING ivfflat: unsupported option {other}"
                )));
            }
        }
    }
    Ok((lists, probes, payload))
}

fn parse_positive_ivfflat_option(option: &LogicalIndexOption) -> Result<usize, ServerError> {
    let parsed = option.value.parse::<usize>().map_err(|_| {
        ServerError::ddl(format!(
            "CREATE INDEX USING ivfflat: option {} must be a positive integer",
            option.name
        ))
    })?;
    if parsed == 0 {
        return Err(ServerError::ddl(format!(
            "CREATE INDEX USING ivfflat: option {} must be greater than zero",
            option.name
        )));
    }
    Ok(parsed)
}
