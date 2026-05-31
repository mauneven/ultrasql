//! Expression-projection operator.
//!
//! Evaluates a list of scalar expressions per child row and emits a
//! batch whose columns are the per-row evaluated values. Unlike the
//! plain [`crate::Project`] operator (which is index-based column
//! routing), this variant handles arbitrary `ScalarExpr` shapes that
//! the binder lowers from a SELECT list — built-in function calls,
//! CASE / COALESCE, arithmetic, etc.

use ultrasql_core::{DataType, Schema, Value, pack_timetz};
use ultrasql_planner::ScalarExpr;
use ultrasql_vec::bitmap::Bitmap;
use ultrasql_vec::column::{BoolColumn, Column, NumericColumn};
use ultrasql_vec::{Batch, DictionaryEncodingPolicy, StringEncoding, encode_strings_auto};

use crate::eval::{EvalError, eval_expr};
use crate::filter_op::batch_to_rows;
use crate::{ExecError, Operator};

/// Pull-mode expression projection.
#[derive(Debug)]
pub struct ProjectExprs {
    child: Box<dyn Operator>,
    schema: Schema,
    exprs: Vec<ScalarExpr>,
    output_types: Vec<DataType>,
    child_schema: Schema,
    done: bool,
}

impl ProjectExprs {
    /// Build a projection operator from the bound `(expr, name)` list
    /// and the resulting output schema (already computed by the
    /// binder).
    pub fn new(
        child: Box<dyn Operator>,
        exprs: &[(ScalarExpr, String)],
        schema: Schema,
    ) -> Result<Self, ExecError> {
        let child_schema = child.schema().clone();
        let scalars: Vec<ScalarExpr> = exprs.iter().map(|(e, _)| e.clone()).collect();
        let output_types: Vec<DataType> = exprs.iter().map(|(e, _)| e.data_type()).collect();
        Ok(Self {
            child,
            schema,
            exprs: scalars,
            output_types,
            child_schema,
            done: false,
        })
    }
}

impl Operator for ProjectExprs {
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.done {
            return Ok(None);
        }
        let Some(batch) = self.child.next_batch()? else {
            self.done = true;
            return Ok(None);
        };
        let rows = batch_to_rows(&batch, &self.child_schema)?;
        let n_rows = rows.len();
        let n_cols = self.exprs.len();
        let mut out_values: Vec<Vec<Value>> =
            (0..n_cols).map(|_| Vec::with_capacity(n_rows)).collect();
        let no_params: &[Value] = &[];
        for row in &rows {
            for (col_idx, expr) in self.exprs.iter().enumerate() {
                let val = eval_expr(expr, row, no_params).map_err(eval_error_to_exec)?;
                out_values[col_idx].push(val);
            }
        }
        let mut columns: Vec<Column> = Vec::with_capacity(n_cols);
        for (col_idx, vals) in out_values.into_iter().enumerate() {
            let dt = &self.output_types[col_idx];
            columns.push(build_column(dt, vals)?);
        }
        Ok(Some(Batch::new(columns)?))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn estimated_row_count(&self) -> Option<usize> {
        self.child.estimated_row_count()
    }

    fn profile_children(&self) -> Vec<&dyn Operator> {
        vec![self.child.as_ref()]
    }
}

fn eval_error_to_exec(error: EvalError) -> ExecError {
    match error {
        EvalError::NumericFieldOverflow(detail) => ExecError::NumericFieldOverflow(detail),
        EvalError::Overflow => ExecError::NumericFieldOverflow("numeric value out of range".into()),
        EvalError::DivByZero => ExecError::DivisionByZero("division by zero".into()),
        EvalError::Type(message) if is_invalid_text_representation(&message) => {
            ExecError::InvalidTextRepresentation(message)
        }
        other => ExecError::TypeMismatch(other.to_string()),
    }
}

fn is_invalid_text_representation(message: &str) -> bool {
    message.starts_with("numeric cast: invalid syntax:")
        || message.starts_with("money cast: invalid")
        || message.starts_with("uuid cast: invalid syntax:")
        || message.starts_with("json cast: invalid JSON:")
        || message.starts_with("jsonb cast: invalid JSON:")
}

/// Assemble a column-oriented batch column from a column-major
/// `Vec<Value>`. The function knows every value type the v0.6 row
/// codec encodes; non-encodable types return a precise error.
fn build_column(dt: &DataType, values: Vec<Value>) -> Result<Column, ExecError> {
    let n = values.len();
    let mut nulls = Bitmap::new(n, true);
    let mut any_null = false;
    macro_rules! numeric_column {
        ($vec_ty:ty, $value_pat:pat => $extract:expr, $default:expr, $column_ctor:ident) => {{
            let mut data: Vec<$vec_ty> = Vec::with_capacity(n);
            for (i, v) in values.iter().enumerate() {
                match v {
                    Value::Null => {
                        nulls.set(i, false);
                        any_null = true;
                        data.push($default);
                    }
                    $value_pat => data.push($extract),
                    other => {
                        return Err(ExecError::TypeMismatch(format!(
                            "projection: expected {dt:?} at row {i}, got {:?}",
                            other.data_type()
                        )));
                    }
                }
            }
            if any_null {
                Ok(Column::$column_ctor(
                    NumericColumn::with_nulls(data, nulls)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?,
                ))
            } else {
                Ok(Column::$column_ctor(NumericColumn::from_data(data)))
            }
        }};
    }
    match dt {
        DataType::Bool => {
            let mut data: Vec<bool> = Vec::with_capacity(n);
            for (i, v) in values.iter().enumerate() {
                match v {
                    Value::Null => {
                        nulls.set(i, false);
                        any_null = true;
                        data.push(false);
                    }
                    Value::Bool(b) => data.push(*b),
                    other => {
                        return Err(ExecError::TypeMismatch(format!(
                            "projection: expected Bool at row {i}, got {:?}",
                            other.data_type()
                        )));
                    }
                }
            }
            if any_null {
                Ok(Column::Bool(
                    BoolColumn::with_nulls(data, nulls)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?,
                ))
            } else {
                Ok(Column::Bool(BoolColumn::from_data(data)))
            }
        }
        DataType::Int16 => {
            // The vec layer has no `Int16` column variant — widen to
            // `Int32` so the downstream wire encoder still works.
            let mut data: Vec<i32> = Vec::with_capacity(n);
            for (i, v) in values.iter().enumerate() {
                match v {
                    Value::Null => {
                        nulls.set(i, false);
                        any_null = true;
                        data.push(0);
                    }
                    Value::Int16(x) => data.push(i32::from(*x)),
                    other => {
                        return Err(ExecError::TypeMismatch(format!(
                            "projection: expected Int16 at row {i}, got {:?}",
                            other.data_type()
                        )));
                    }
                }
            }
            if any_null {
                Ok(Column::Int32(
                    NumericColumn::with_nulls(data, nulls)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?,
                ))
            } else {
                Ok(Column::Int32(NumericColumn::from_data(data)))
            }
        }
        DataType::Int32 => numeric_column!(i32, Value::Int32(v) => *v, 0_i32, Int32),
        DataType::Int64 => numeric_column!(
            i64,
            Value::Int64(v) | Value::Timestamp(v) | Value::TimestampTz(v) | Value::Time(v) => *v,
            0_i64,
            Int64
        ),
        DataType::Float32 => numeric_column!(f32, Value::Float32(v) => *v, 0.0_f32, Float32),
        DataType::Float64 => numeric_column!(f64, Value::Float64(v) => *v, 0.0_f64, Float64),
        DataType::Date => numeric_column!(i32, Value::Date(v) => *v, 0_i32, Int32),
        DataType::Decimal { scale: None, .. } => {
            let mut strings: Vec<Option<String>> = Vec::with_capacity(n);
            for (i, v) in values.iter().enumerate() {
                match v {
                    Value::Null => strings.push(None),
                    Value::Decimal { .. } => strings.push(Some(v.to_string())),
                    other => {
                        return Err(ExecError::TypeMismatch(format!(
                            "projection: expected {dt:?} at row {i}, got {:?}",
                            other.data_type()
                        )));
                    }
                }
            }
            Ok(
                match encode_strings_auto(
                    strings.iter().map(|v| v.as_deref()),
                    DictionaryEncodingPolicy::default(),
                ) {
                    StringEncoding::Raw(c) => Column::Utf8(c),
                    StringEncoding::Dictionary(c) => Column::DictionaryUtf8(c),
                },
            )
        }
        DataType::Decimal { .. } => numeric_column!(
            i64,
            Value::Decimal { value, .. } => *value,
            0_i64,
            Int64
        ),
        DataType::Money => numeric_column!(i64, Value::Money(v) => *v, 0_i64, Int64),
        DataType::Oid | DataType::RegClass | DataType::RegType => {
            let mut data: Vec<i64> = Vec::with_capacity(n);
            for (i, v) in values.iter().enumerate() {
                match (dt, v) {
                    (_, Value::Null) => {
                        nulls.set(i, false);
                        any_null = true;
                        data.push(0);
                    }
                    (DataType::Oid, Value::Oid(oid))
                    | (DataType::RegClass, Value::RegClass(oid))
                    | (DataType::RegType, Value::RegType(oid)) => data.push(i64::from(oid.raw())),
                    other => {
                        return Err(ExecError::TypeMismatch(format!(
                            "projection: expected {dt:?} at row {i}, got {:?}",
                            other.1.data_type()
                        )));
                    }
                }
            }
            if any_null {
                Ok(Column::Int64(
                    NumericColumn::with_nulls(data, nulls)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?,
                ))
            } else {
                Ok(Column::Int64(NumericColumn::from_data(data)))
            }
        }
        DataType::Timestamp | DataType::TimestampTz | DataType::Time => numeric_column!(
            i64,
            Value::Timestamp(v) | Value::TimestampTz(v) | Value::Time(v) => *v,
            0_i64,
            Int64
        ),
        DataType::TimeTz => {
            let mut data: Vec<i64> = Vec::with_capacity(n);
            for (i, v) in values.iter().enumerate() {
                match v {
                    Value::Null => {
                        nulls.set(i, false);
                        any_null = true;
                        data.push(0);
                    }
                    Value::TimeTz {
                        micros,
                        offset_seconds,
                    } => data.push(pack_timetz(*micros, *offset_seconds).ok_or_else(|| {
                        ExecError::TypeMismatch(format!("projection: invalid TimeTz at row {i}"))
                    })?),
                    other => {
                        return Err(ExecError::TypeMismatch(format!(
                            "projection: expected {dt:?} at row {i}, got {:?}",
                            other.data_type()
                        )));
                    }
                }
            }
            if any_null {
                Ok(Column::Int64(
                    NumericColumn::with_nulls(data, nulls)
                        .map_err(|e| ExecError::TypeMismatch(e.to_string()))?,
                ))
            } else {
                Ok(Column::Int64(NumericColumn::from_data(data)))
            }
        }
        DataType::Text { .. }
        | DataType::Enum { .. }
        | DataType::Composite { .. }
        | DataType::Char { .. }
        | DataType::Bit { .. }
        | DataType::VarBit { .. }
        | DataType::Inet
        | DataType::Cidr
        | DataType::MacAddr
        | DataType::MacAddr8
        | DataType::PgLsn => {
            let mut strings: Vec<Option<String>> = Vec::with_capacity(n);
            for (i, v) in values.iter().enumerate() {
                match (dt, v) {
                    (_, Value::Null) => strings.push(None),
                    (DataType::Text { .. }, Value::Text(s))
                    | (DataType::Enum { .. }, Value::Text(s))
                    | (DataType::Composite { .. }, Value::Text(s))
                    | (DataType::Char { .. }, Value::Char(s)) => strings.push(Some(s.clone())),
                    (DataType::PgLsn, Value::PgLsn(lsn)) => strings.push(Some(lsn.to_string())),
                    (DataType::Bit { .. } | DataType::VarBit { .. }, Value::BitString(bits))
                        if bits.matches_type(dt) =>
                    {
                        strings.push(Some(bits.to_string()));
                    }
                    (
                        DataType::Inet | DataType::Cidr | DataType::MacAddr | DataType::MacAddr8,
                        Value::Network(network),
                    ) if network.data_type() == *dt => {
                        strings.push(Some(network.to_string()));
                    }
                    other => {
                        return Err(ExecError::TypeMismatch(format!(
                            "projection: expected {dt} at row {i}, got {:?}",
                            other.1.data_type()
                        )));
                    }
                }
            }
            Ok(
                match encode_strings_auto(
                    strings.iter().map(|v| v.as_deref()),
                    DictionaryEncodingPolicy::default(),
                ) {
                    StringEncoding::Raw(c) => Column::Utf8(c),
                    StringEncoding::Dictionary(c) => Column::DictionaryUtf8(c),
                },
            )
        }
        DataType::Json | DataType::Jsonb | DataType::Xml => {
            let mut strings: Vec<Option<String>> = Vec::with_capacity(n);
            for (i, v) in values.iter().enumerate() {
                match (dt, v) {
                    (_, Value::Null) => strings.push(None),
                    (DataType::Json, Value::Json(s))
                    | (DataType::Jsonb, Value::Jsonb(s))
                    | (DataType::Xml, Value::Xml(s)) => strings.push(Some(s.clone())),
                    other => {
                        return Err(ExecError::TypeMismatch(format!(
                            "projection: expected {dt} at row {i}, got {:?}",
                            other.1.data_type()
                        )));
                    }
                }
            }
            Ok(
                match encode_strings_auto(
                    strings.iter().map(|v| v.as_deref()),
                    DictionaryEncodingPolicy::default(),
                ) {
                    StringEncoding::Raw(c) => Column::Utf8(c),
                    StringEncoding::Dictionary(c) => Column::DictionaryUtf8(c),
                },
            )
        }
        ty if ty.is_vector_family() => {
            let mut strings: Vec<Option<String>> = Vec::with_capacity(n);
            for (i, v) in values.iter().enumerate() {
                match v {
                    Value::Null => strings.push(None),
                    value if vector_family_value_matches(ty, value) => {
                        strings.push(Some(v.to_string()));
                    }
                    other => {
                        return Err(ExecError::TypeMismatch(format!(
                            "projection: expected {dt:?} at row {i}, got {:?}",
                            other.data_type()
                        )));
                    }
                }
            }
            Ok(
                match encode_strings_auto(
                    strings.iter().map(|v| v.as_deref()),
                    DictionaryEncodingPolicy::default(),
                ) {
                    StringEncoding::Raw(c) => Column::Utf8(c),
                    StringEncoding::Dictionary(c) => Column::DictionaryUtf8(c),
                },
            )
        }
        DataType::Array(expected) => {
            let mut strings: Vec<Option<String>> = Vec::with_capacity(n);
            for (i, v) in values.iter().enumerate() {
                match v {
                    Value::Null => strings.push(None),
                    Value::Array { element_type, .. } if expected.as_ref() == element_type => {
                        strings.push(Some(v.to_string()));
                    }
                    other => {
                        return Err(ExecError::TypeMismatch(format!(
                            "projection: expected Array at row {i}, got {:?}",
                            other.data_type()
                        )));
                    }
                }
            }
            Ok(
                match encode_strings_auto(
                    strings.iter().map(|v| v.as_deref()),
                    DictionaryEncodingPolicy::default(),
                ) {
                    StringEncoding::Raw(c) => Column::Utf8(c),
                    StringEncoding::Dictionary(c) => Column::DictionaryUtf8(c),
                },
            )
        }
        DataType::Uuid => {
            let mut strings: Vec<Option<String>> = Vec::with_capacity(n);
            for (i, v) in values.iter().enumerate() {
                match v {
                    Value::Null => strings.push(None),
                    Value::Uuid(bytes) => strings.push(Some(Value::Uuid(*bytes).to_string())),
                    other => {
                        return Err(ExecError::TypeMismatch(format!(
                            "projection: expected Uuid at row {i}, got {:?}",
                            other.data_type()
                        )));
                    }
                }
            }
            Ok(
                match encode_strings_auto(
                    strings.iter().map(|v| v.as_deref()),
                    DictionaryEncodingPolicy::default(),
                ) {
                    StringEncoding::Raw(c) => Column::Utf8(c),
                    StringEncoding::Dictionary(c) => Column::DictionaryUtf8(c),
                },
            )
        }
        DataType::Bytea => {
            let mut strings: Vec<Option<String>> = Vec::with_capacity(n);
            for (i, v) in values.iter().enumerate() {
                match v {
                    Value::Null => strings.push(None),
                    Value::Bytea(bytes) => {
                        strings.push(Some(Value::Bytea(bytes.clone()).to_string()))
                    }
                    other => {
                        return Err(ExecError::TypeMismatch(format!(
                            "projection: expected Bytea at row {i}, got {:?}",
                            other.data_type()
                        )));
                    }
                }
            }
            Ok(
                match encode_strings_auto(
                    strings.iter().map(|v| v.as_deref()),
                    DictionaryEncodingPolicy::default(),
                ) {
                    StringEncoding::Raw(c) => Column::Utf8(c),
                    StringEncoding::Dictionary(c) => Column::DictionaryUtf8(c),
                },
            )
        }
        DataType::Null => {
            // All-NULL output column. Carry an Int32 storage; the
            // schema field tag still reports `Null`.
            let data: Vec<i32> = vec![0_i32; n];
            let mut bm = Bitmap::new(n, false);
            for i in 0..n {
                bm.set(i, false);
            }
            Ok(Column::Int32(
                NumericColumn::with_nulls(data, bm)
                    .map_err(|e| ExecError::TypeMismatch(e.to_string()))?,
            ))
        }
        other => Err(ExecError::TypeMismatch(format!(
            "projection: column type {other:?} not yet supported"
        ))),
    }
}

fn vector_family_value_matches(expected: &DataType, value: &Value) -> bool {
    let actual = value.data_type();
    vector_family_kind(expected) == vector_family_kind(&actual)
        && dims_compatible(
            expected.vector_dims().flatten(),
            actual.vector_dims().flatten(),
        )
}

fn vector_family_kind(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Vector { .. } => Some(0),
        DataType::HalfVec { .. } => Some(1),
        DataType::SparseVec { .. } => Some(2),
        DataType::BitVec { .. } => Some(3),
        _ => None,
    }
}

const fn dims_compatible(left: Option<u32>, right: Option<u32>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left == right,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ultrasql_core::{
        BitString, DataType, Lsn, NetworkValue, Oid, SparseVector, Value, pack_timetz,
    };
    use ultrasql_vec::column::Column;

    use super::{build_column, vector_family_value_matches};

    fn assert_i32_values(column: Column, expected: &[i32], null_at: Option<usize>) {
        let Column::Int32(c) = column else {
            panic!("expected Int32 column");
        };
        assert_eq!(c.data(), expected);
        if let Some(row) = null_at {
            assert!(!c.nulls().expect("nulls").get(row));
        } else {
            assert!(c.nulls().is_none());
        }
    }

    fn assert_i64_values(column: Column, expected: &[i64], null_at: Option<usize>) {
        let Column::Int64(c) = column else {
            panic!("expected Int64 column");
        };
        assert_eq!(c.data(), expected);
        if let Some(row) = null_at {
            assert!(!c.nulls().expect("nulls").get(row));
        } else {
            assert!(c.nulls().is_none());
        }
    }

    fn assert_text_values(column: Column, expected: &[Option<&str>]) {
        assert_eq!(column.len(), expected.len());
        for (row, expected_value) in expected.iter().enumerate() {
            assert_eq!(column.text_value(row), *expected_value);
        }
    }

    #[test]
    fn build_column_covers_numeric_bool_temporal_and_oid_families() {
        let bool_col = build_column(
            &DataType::Bool,
            vec![Value::Bool(true), Value::Null, Value::Bool(false)],
        )
        .expect("bool column");
        let Column::Bool(c) = bool_col else {
            panic!("expected bool column");
        };
        assert_eq!(c.data(), &[1, 0, 0]);
        assert!(!c.nulls().expect("nulls").get(1));

        assert_i32_values(
            build_column(&DataType::Int16, vec![Value::Int16(7), Value::Null]).expect("int16"),
            &[7, 0],
            Some(1),
        );
        assert_i32_values(
            build_column(&DataType::Int32, vec![Value::Int32(11), Value::Int32(12)])
                .expect("int32"),
            &[11, 12],
            None,
        );
        assert_i64_values(
            build_column(&DataType::Int64, vec![Value::Int64(9), Value::Null]).expect("int64"),
            &[9, 0],
            Some(1),
        );
        assert_i64_values(
            build_column(
                &DataType::TimeTz,
                vec![
                    Value::TimeTz {
                        micros: 12_345,
                        offset_seconds: -18_000,
                    },
                    Value::Null,
                ],
            )
            .expect("timetz"),
            &[pack_timetz(12_345, -18_000).expect("packed"), 0],
            Some(1),
        );
        assert_i32_values(
            build_column(&DataType::Date, vec![Value::Date(42)]).expect("date"),
            &[42],
            None,
        );
        assert_i64_values(
            build_column(
                &DataType::Decimal {
                    precision: Some(10),
                    scale: Some(2),
                },
                vec![Value::Decimal {
                    value: 1234,
                    scale: 2,
                }],
            )
            .expect("decimal"),
            &[1234],
            None,
        );
        assert_text_values(
            build_column(
                &DataType::Decimal {
                    precision: None,
                    scale: None,
                },
                vec![
                    Value::Decimal {
                        value: 5678,
                        scale: 2,
                    },
                    Value::Null,
                    Value::Decimal {
                        value: 35,
                        scale: 1,
                    },
                ],
            )
            .expect("dynamic decimal"),
            &[Some("56.78"), None, Some("3.5")],
        );
        assert_i64_values(
            build_column(&DataType::Money, vec![Value::Money(999)]).expect("money"),
            &[999],
            None,
        );
        assert_i64_values(
            build_column(
                &DataType::RegClass,
                vec![Value::RegClass(Oid::new(77)), Value::Null],
            )
            .expect("regclass"),
            &[77, 0],
            Some(1),
        );
    }

    #[test]
    fn build_column_covers_textual_json_vector_array_uuid_and_bytea_families() {
        assert_text_values(
            build_column(
                &DataType::Enum {
                    oid: Oid::new(5000),
                    name: Arc::from("mood"),
                    labels: Arc::from([String::from("ok")]),
                },
                vec![Value::Text("ok".to_owned()), Value::Null],
            )
            .expect("enum"),
            &[Some("ok"), None],
        );
        assert_text_values(
            build_column(
                &DataType::Bit { len: Some(4) },
                vec![Value::BitString(BitString::parse("1010").expect("bits"))],
            )
            .expect("bit"),
            &[Some("1010")],
        );
        assert_text_values(
            build_column(
                &DataType::Inet,
                vec![Value::Network(
                    NetworkValue::parse_for_type(&DataType::Inet, "127.0.0.1").expect("inet"),
                )],
            )
            .expect("inet"),
            &[Some("127.0.0.1")],
        );
        assert_text_values(
            build_column(
                &DataType::PgLsn,
                vec![Value::PgLsn(Lsn::new(0x16_B374_D848))],
            )
            .expect("pg_lsn"),
            &[Some("16/B374D848")],
        );
        assert_text_values(
            build_column(
                &DataType::Jsonb,
                vec![Value::Jsonb("{\"a\":1}".to_owned()), Value::Null],
            )
            .expect("jsonb"),
            &[Some("{\"a\":1}"), None],
        );
        assert_text_values(
            build_column(
                &DataType::Vector { dims: Some(2) },
                vec![Value::Vector(vec![1.0, 2.0])],
            )
            .expect("vector"),
            &[Some("[1,2]")],
        );
        assert_text_values(
            build_column(
                &DataType::SparseVec { dims: Some(4) },
                vec![Value::SparseVec(
                    SparseVector::new(4, vec![(1, 1.5)]).expect("sparse"),
                )],
            )
            .expect("sparsevec"),
            &[Some("{1:1.5}/4")],
        );
        assert_text_values(
            build_column(
                &DataType::Array(Box::new(DataType::Int32)),
                vec![Value::Array {
                    element_type: DataType::Int32,
                    elements: vec![Value::Int32(1), Value::Int32(2)],
                }],
            )
            .expect("array"),
            &[Some("{1,2}")],
        );
        assert_text_values(
            build_column(&DataType::Uuid, vec![Value::Uuid([1; 16])]).expect("uuid"),
            &[Some("01010101-0101-0101-0101-010101010101")],
        );
        assert_text_values(
            build_column(&DataType::Bytea, vec![Value::Bytea(vec![0xde, 0xad])]).expect("bytea"),
            &[Some("\\xdead")],
        );
        assert_i32_values(
            build_column(&DataType::Null, vec![Value::Null, Value::Null]).expect("null"),
            &[0, 0],
            Some(0),
        );
    }

    #[test]
    fn build_column_rejects_type_mismatch_and_bad_vector_dimensions() {
        let err = build_column(&DataType::Int32, vec![Value::Text("x".to_owned())])
            .expect_err("type mismatch");
        assert!(err.to_string().contains("expected Int32"));

        let err = build_column(
            &DataType::Vector { dims: Some(3) },
            vec![Value::Vector(vec![1.0, 2.0])],
        )
        .expect_err("dimension mismatch");
        assert!(err.to_string().contains("expected Vector"));

        assert!(vector_family_value_matches(
            &DataType::HalfVec { dims: Some(2) },
            &Value::HalfVec(vec![1.0, 2.0])
        ));
        assert!(!vector_family_value_matches(
            &DataType::HalfVec { dims: Some(3) },
            &Value::HalfVec(vec![1.0, 2.0])
        ));
    }
}
