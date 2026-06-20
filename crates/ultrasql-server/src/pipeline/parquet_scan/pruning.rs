//! Row-group selection via statistics and dictionary-page pruning.

use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use parquet::basic::{Encoding, Type as ParquetPhysicalType};
use parquet::column::page::Page;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::{ChunkReader, SerializedPageReader};
use parquet::file::statistics::Statistics;
use ultrasql_planner::BinaryOp;

use crate::error::ServerError;

use super::ParquetRowGroupSummary;
use super::predicate::{ParquetLiteral, ParquetPredicate};

pub(super) fn selected_row_groups_with_dictionary<R>(
    reader: Arc<R>,
    metadata: &ParquetMetaData,
    schema: &ArrowSchema,
    predicate: Option<&ParquetPredicate>,
) -> Result<Vec<usize>, ServerError>
where
    R: ChunkReader + 'static,
{
    if let Some(predicate) = predicate {
        return select_row_groups(metadata, schema, predicate, |row_group, column| {
            dictionary_page_may_match(Arc::clone(&reader), metadata, row_group, column, predicate)
        });
    }
    Ok((0..metadata.num_row_groups()).collect())
}

pub(super) fn row_group_summary_with_dictionary<R>(
    reader: Arc<R>,
    metadata: &ParquetMetaData,
    schema: &ArrowSchema,
    predicate: Option<&ParquetPredicate>,
) -> Result<ParquetRowGroupSummary, ServerError>
where
    R: ChunkReader + 'static,
{
    let total = metadata.num_row_groups();
    let selected = selected_row_groups_with_dictionary(reader, metadata, schema, predicate)?.len();
    let skipped = total.saturating_sub(selected);
    Ok(ParquetRowGroupSummary {
        scanned: u64::try_from(selected).unwrap_or(u64::MAX),
        skipped: u64::try_from(skipped).unwrap_or(u64::MAX),
    })
}

fn select_row_groups(
    metadata: &ParquetMetaData,
    schema: &ArrowSchema,
    predicate: &ParquetPredicate,
    mut dictionary_may_match: impl FnMut(usize, usize) -> Result<bool, ServerError>,
) -> Result<Vec<usize>, ServerError> {
    let Some(column_index) = schema
        .fields()
        .iter()
        .position(|field| field.name() == &predicate.column)
    else {
        return Err(ServerError::CopyFormat(format!(
            "read_parquet predicate column not found: {}",
            predicate.column
        )));
    };
    let mut row_groups = Vec::new();
    for index in 0..metadata.num_row_groups() {
        let row_group = metadata.row_group(index);
        let stats = row_group.column(column_index).statistics();
        let row_count = parquet_row_group_row_count(row_group);
        if stats.is_some_and(|stats| !statistics_may_match(stats, predicate, row_count)) {
            continue;
        }
        if !dictionary_may_match(index, column_index)? {
            continue;
        }
        row_groups.push(index);
    }
    Ok(row_groups)
}

fn statistics_may_match(stats: &Statistics, predicate: &ParquetPredicate, row_count: u64) -> bool {
    if row_count > 0
        && stats
            .null_count_opt()
            .is_some_and(|nulls| nulls >= row_count)
    {
        return false;
    }
    match (stats, &predicate.literal) {
        (Statistics::Boolean(stats), ParquetLiteral::Bool(value)) => {
            range_may_match(stats.min_opt(), stats.max_opt(), predicate.op, value)
        }
        (Statistics::Int32(stats), ParquetLiteral::Int64(value)) => {
            let min = stats.min_opt().map(|v| i64::from(*v));
            let max = stats.max_opt().map(|v| i64::from(*v));
            range_may_match(min.as_ref(), max.as_ref(), predicate.op, value)
        }
        (Statistics::Int64(stats), ParquetLiteral::Int64(value)) => {
            range_may_match(stats.min_opt(), stats.max_opt(), predicate.op, value)
        }
        (Statistics::Float(stats), ParquetLiteral::Float64(value)) => {
            let min = stats.min_opt().map(|v| f64::from(*v));
            let max = stats.max_opt().map(|v| f64::from(*v));
            range_may_match(min.as_ref(), max.as_ref(), predicate.op, value)
        }
        (Statistics::Double(stats), ParquetLiteral::Float64(value)) => {
            range_may_match(stats.min_opt(), stats.max_opt(), predicate.op, value)
        }
        (Statistics::ByteArray(stats), ParquetLiteral::Text(value)) => {
            let min = stats.min_opt().map(parquet::data_type::ByteArray::data);
            let max = stats.max_opt().map(parquet::data_type::ByteArray::data);
            range_may_match(min, max, predicate.op, value.as_bytes())
        }
        _ => true,
    }
}

fn parquet_row_group_row_count(row_group: &parquet::file::metadata::RowGroupMetaData) -> u64 {
    u64::try_from(row_group.num_rows()).unwrap_or(0)
}

fn dictionary_page_may_match<R>(
    reader: Arc<R>,
    metadata: &ParquetMetaData,
    row_group_index: usize,
    column_index: usize,
    predicate: &ParquetPredicate,
) -> Result<bool, ServerError>
where
    R: ChunkReader + 'static,
{
    if predicate.op != BinaryOp::Eq {
        return Ok(true);
    }
    let row_group = metadata.row_group(row_group_index);
    let column = row_group.column(column_index);
    if !column_chunk_is_dictionary_prunable(column) {
        return Ok(true);
    }
    let total_rows = usize::try_from(row_group.num_rows()).unwrap_or(0);
    let mut page_reader =
        SerializedPageReader::new(reader, column, total_rows, None).map_err(|err| {
            ServerError::CopyFormat(format!(
                "read_parquet cannot inspect dictionary for row group {row_group_index}: {err}"
            ))
        })?;
    if let Some(page) = page_reader.next() {
        let page = page.map_err(|err| {
            ServerError::CopyFormat(format!(
                "read_parquet cannot inspect dictionary for row group {row_group_index}: {err}"
            ))
        })?;
        match page {
            Page::DictionaryPage { .. } => {
                return Ok(
                    dictionary_contains_literal(&page, column.column_type(), predicate)
                        .unwrap_or(true),
                );
            }
            Page::DataPage { .. } | Page::DataPageV2 { .. } => return Ok(true),
        }
    }
    Ok(true)
}

fn column_chunk_is_dictionary_prunable(
    column: &parquet::file::metadata::ColumnChunkMetaData,
) -> bool {
    column.dictionary_page_offset().is_some()
        && column.page_encoding_stats_mask().is_some_and(|mask| {
            mask.is_only(Encoding::PLAIN_DICTIONARY) || mask.is_only(Encoding::RLE_DICTIONARY)
        })
}

fn dictionary_contains_literal(
    page: &Page,
    physical_type: ParquetPhysicalType,
    predicate: &ParquetPredicate,
) -> Option<bool> {
    let Page::DictionaryPage {
        buf,
        num_values,
        encoding,
        ..
    } = page
    else {
        return None;
    };
    if *encoding != Encoding::PLAIN {
        return None;
    }
    match (physical_type, &predicate.literal) {
        (ParquetPhysicalType::BYTE_ARRAY, ParquetLiteral::Text(value)) => {
            plain_byte_array_dictionary_contains(buf, *num_values, value.as_bytes())
        }
        (ParquetPhysicalType::INT32, ParquetLiteral::Int64(value)) => {
            let needle = i32::try_from(*value).ok()?;
            plain_i32_dictionary_contains(buf, *num_values, needle)
        }
        (ParquetPhysicalType::INT64, ParquetLiteral::Int64(value)) => {
            plain_i64_dictionary_contains(buf, *num_values, *value)
        }
        (ParquetPhysicalType::DOUBLE, ParquetLiteral::Float64(value)) => {
            plain_f64_dictionary_contains(buf, *num_values, *value)
        }
        _ => None,
    }
}

fn plain_byte_array_dictionary_contains(
    buf: &[u8],
    num_values: u32,
    needle: &[u8],
) -> Option<bool> {
    let mut offset = 0_usize;
    for _ in 0..usize::try_from(num_values).ok()? {
        let len_bytes = buf.get(offset..offset.checked_add(4)?)?;
        let len = usize::try_from(u32::from_le_bytes(len_bytes.try_into().ok()?)).ok()?;
        offset = offset.checked_add(4)?;
        let end = offset.checked_add(len)?;
        let value = buf.get(offset..end)?;
        if value == needle {
            return Some(true);
        }
        offset = end;
    }
    Some(false)
}

fn plain_i32_dictionary_contains(buf: &[u8], num_values: u32, needle: i32) -> Option<bool> {
    plain_fixed_dictionary_contains(buf, num_values, 4, |bytes| {
        let Ok(bytes) = <[u8; 4]>::try_from(bytes) else {
            return false;
        };
        i32::from_le_bytes(bytes) == needle
    })
}

fn plain_i64_dictionary_contains(buf: &[u8], num_values: u32, needle: i64) -> Option<bool> {
    plain_fixed_dictionary_contains(buf, num_values, 8, |bytes| {
        let Ok(bytes) = <[u8; 8]>::try_from(bytes) else {
            return false;
        };
        i64::from_le_bytes(bytes) == needle
    })
}

fn plain_f64_dictionary_contains(buf: &[u8], num_values: u32, needle: f64) -> Option<bool> {
    plain_fixed_dictionary_contains(buf, num_values, 8, |bytes| {
        let Ok(bytes) = <[u8; 8]>::try_from(bytes) else {
            return false;
        };
        f64::from_le_bytes(bytes) == needle
    })
}

fn plain_fixed_dictionary_contains(
    buf: &[u8],
    num_values: u32,
    width: usize,
    mut matches: impl FnMut(&[u8]) -> bool,
) -> Option<bool> {
    let count = usize::try_from(num_values).ok()?;
    for idx in 0..count {
        let start = idx.checked_mul(width)?;
        let end = start.checked_add(width)?;
        if matches(buf.get(start..end)?) {
            return Some(true);
        }
    }
    Some(false)
}

fn range_may_match<T: PartialOrd + PartialEq + ?Sized>(
    min: Option<&T>,
    max: Option<&T>,
    op: BinaryOp,
    value: &T,
) -> bool {
    match op {
        BinaryOp::Eq => {
            if min.is_some_and(|min| value < min) {
                return false;
            }
            if max.is_some_and(|max| value > max) {
                return false;
            }
            true
        }
        BinaryOp::NotEq => {
            !(min.is_some_and(|min| min == value) && max.is_some_and(|max| max == value))
        }
        BinaryOp::Lt => min.is_none_or(|min| min < value),
        BinaryOp::LtEq => min.is_none_or(|min| min <= value),
        BinaryOp::Gt => max.is_none_or(|max| max > value),
        BinaryOp::GtEq => max.is_none_or(|max| max >= value),
        _ => true,
    }
}
