//! `COPY` binding for both table targets and `COPY (SELECT ...)` query
//! targets.

use ultrasql_core::{Field, Schema};
use ultrasql_parser::ast::{
    CopyDirection as AstCopyDirection, CopyFormat as AstCopyFormat, CopyOption,
    CopySource as AstCopySource, CopyStmt,
};

use super::super::{
    Catalog, LogicalPlan, PlanError, ScopeStack, bind_select, lookup_table_reference,
};
use crate::plan::{CopyDirection, CopyFormat, CopySource};

/// Bind a `COPY` statement.
///
/// Validates the target table, resolves every column name in the optional
/// `(col_list)` against the table's schema, and folds the parsed
/// `WITH (…)` options into the format-appropriate defaults (`\t` delimiter
/// + `\N` NULL marker for TEXT; `,` delimiter + empty-string NULL marker
/// for CSV). The produced [`LogicalPlan::Copy`] carries the row-shape
/// schema the server's session dispatcher needs to encode `CopyOutResponse`
/// / `CopyInResponse` frames.
pub(in crate::binder) fn bind_copy(
    s: &CopyStmt,
    catalog: &dyn Catalog,
) -> Result<LogicalPlan, PlanError> {
    if let Some(query) = &s.query {
        if !s.columns.is_empty() {
            return Err(PlanError::NotSupported(
                "COPY query target cannot specify a column list",
            ));
        }
        let mut scope = ScopeStack::new();
        let input = bind_select(query, catalog, &mut scope)?;
        let schema = input.schema().clone();
        let direction = match s.direction {
            AstCopyDirection::From => {
                return Err(PlanError::NotSupported(
                    "COPY (SELECT ...) supports TO only",
                ));
            }
            AstCopyDirection::To => CopyDirection::To,
        };
        let source = match &s.source {
            AstCopySource::Stdout => CopySource::Stdout,
            AstCopySource::File(path) => CopySource::File(path.clone()),
            AstCopySource::Stdin => {
                return Err(PlanError::NotSupported(
                    "COPY query target cannot use STDIN",
                ));
            }
        };
        let format = match s.format {
            AstCopyFormat::Text => CopyFormat::Text,
            AstCopyFormat::Csv => CopyFormat::Csv,
            AstCopyFormat::Binary => CopyFormat::Binary,
            AstCopyFormat::Parquet => CopyFormat::Parquet,
        };
        let (mut delimiter, mut null_str) = match format {
            CopyFormat::Text | CopyFormat::Binary | CopyFormat::Parquet => {
                ('\t', String::from(r"\N"))
            }
            CopyFormat::Csv => (',', String::new()),
        };
        let mut header = false;
        let mut auto_detect = false;
        let mut ignore_errors = false;
        let mut max_errors = 0_u64;
        let mut reject_table = None;
        for opt in &s.options {
            match opt {
                CopyOption::Format(_) => {}
                CopyOption::Delimiter(c) => delimiter = *c,
                CopyOption::Header(v) => header = *v,
                CopyOption::AutoDetect(v) => auto_detect = *v,
                CopyOption::IgnoreErrors(v) => ignore_errors = *v,
                CopyOption::MaxErrors(v) => max_errors = *v,
                CopyOption::RejectTable(v) => reject_table = Some(v.to_ascii_lowercase()),
                CopyOption::Null(v) => null_str.clone_from(v),
            }
        }
        return Ok(LogicalPlan::Copy {
            relation: None,
            input: Some(Box::new(input)),
            columns: Vec::new(),
            direction,
            source,
            format,
            delimiter,
            null_str,
            header,
            auto_detect,
            ignore_errors,
            max_errors,
            reject_table,
            schema,
        });
    }

    let table_name = s.table.as_ref().ok_or(PlanError::NotSupported(
        "COPY requires table or query target",
    ))?;
    let relation = lookup_table_reference(catalog, table_name)?;
    let table_meta = relation.meta;

    let columns: Vec<usize> = if s.columns.is_empty() {
        Vec::new()
    } else {
        let mut indices = Vec::with_capacity(s.columns.len());
        for ident in &s.columns {
            let folded = ident.value.to_ascii_lowercase();
            let idx = table_meta
                .schema
                .fields()
                .iter()
                .position(|f| f.name.eq_ignore_ascii_case(&folded))
                .ok_or_else(|| PlanError::ColumnNotFound(ident.value.clone()))?;
            indices.push(idx);
        }
        indices
    };

    let stream_schema = if columns.is_empty() {
        table_meta.schema.clone()
    } else {
        let fields: Vec<Field> = columns
            .iter()
            .map(|&i| table_meta.schema.fields()[i].clone())
            .collect();
        Schema::new(fields)
            .map_err(|e| PlanError::TypeMismatch(format!("COPY column projection: {e}")))?
    };

    let direction = match s.direction {
        AstCopyDirection::From => CopyDirection::From,
        AstCopyDirection::To => CopyDirection::To,
    };
    let source = match &s.source {
        AstCopySource::Stdin => CopySource::Stdin,
        AstCopySource::Stdout => CopySource::Stdout,
        AstCopySource::File(path) => CopySource::File(path.clone()),
    };
    let mut format = match s.format {
        AstCopyFormat::Text => CopyFormat::Text,
        AstCopyFormat::Csv => CopyFormat::Csv,
        AstCopyFormat::Binary => CopyFormat::Binary,
        AstCopyFormat::Parquet => CopyFormat::Parquet,
    };
    if !copy_has_explicit_format(&s.options) {
        if let AstCopySource::File(path) = &s.source {
            if copy_file_extension_is(path, "parquet") {
                format = CopyFormat::Parquet;
            }
        }
    }

    let (mut delimiter, mut null_str) = match format {
        CopyFormat::Text | CopyFormat::Binary | CopyFormat::Parquet => ('\t', String::from(r"\N")),
        CopyFormat::Csv => (',', String::new()),
    };
    let mut header = false;
    let mut auto_detect = false;
    let mut ignore_errors = false;
    let mut max_errors = 0_u64;
    let mut reject_table = None;
    for opt in &s.options {
        match opt {
            CopyOption::Format(_) => { /* applied above */ }
            CopyOption::Delimiter(c) => delimiter = *c,
            CopyOption::Header(v) => header = *v,
            CopyOption::AutoDetect(v) => auto_detect = *v,
            CopyOption::IgnoreErrors(v) => ignore_errors = *v,
            CopyOption::MaxErrors(v) => max_errors = *v,
            CopyOption::RejectTable(v) => reject_table = Some(v.to_ascii_lowercase()),
            CopyOption::Null(v) => null_str.clone_from(v),
        }
    }

    Ok(LogicalPlan::Copy {
        relation: Some(relation.plan_name),
        input: None,
        columns,
        direction,
        source,
        format,
        delimiter,
        null_str,
        header,
        auto_detect,
        ignore_errors,
        max_errors,
        reject_table,
        schema: stream_schema,
    })
}

fn copy_has_explicit_format(options: &[CopyOption]) -> bool {
    options
        .iter()
        .any(|option| matches!(option, CopyOption::Format(_)))
}

fn copy_file_extension_is(path: &str, extension: &str) -> bool {
    std::path::Path::new(path)
        .extension()
        .and_then(std::ffi::OsStr::to_str)
        .is_some_and(|ext| ext.eq_ignore_ascii_case(extension))
}
