//! Set-returning function (SRF) scan operator.
//!
//! [`FunctionScan`] evaluates a set-returning function and emits its rows
//! as batches.
//!
//! # `generate_series`
//!
//! `generate_series(start, stop, step)` emits integer rows from `start` to
//! `stop` (inclusive) stepping by `step`. Negative `step` allows
//! descending series. A zero `step` returns [`ExecError::InvalidParameterValue`].
//!
//! The output schema has a single `Int64` column named `generate_series`.
//!
use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn};

use crate::seq_scan::build_batch;
use crate::{CancelFlag, ExecError, Operator};

const BATCH_TARGET_ROWS: usize = 4096;

/// The supported set-returning functions.
#[derive(Debug, Clone)]
pub enum SrfKind {
    /// `generate_series(start, stop, step)`.
    GenerateSeries {
        /// First value to emit.
        start: i64,
        /// Last value to emit (inclusive).
        stop: i64,
        /// Step between successive values. Zero emits nothing.
        step: i64,
    },
    /// `unnest(anyarray)`.
    Unnest {
        /// Values to emit.
        elements: Vec<Value>,
    },
}

/// Set-returning function scan operator.
///
/// Emits the rows produced by the named SRF in 4096-row batches.
///
/// # Send
///
/// All owned state is `Send`.
#[derive(Debug)]
pub struct FunctionScan {
    kind: SrfKind,
    schema: Schema,
    /// Current value for `generate_series`.
    current: i64,
    /// Current offset for `unnest`.
    position: usize,
    cancel_flag: Option<CancelFlag>,
    eof: bool,
}

impl FunctionScan {
    /// Construct a `generate_series(start, stop, step)` scan.
    ///
    /// The output schema has a single `Int64` column named `generate_series`.
    #[must_use]
    #[allow(clippy::similar_names)] // `stop` and `step` are standard generate_series parameter names
    pub fn generate_series(start: i64, stop: i64, step: i64) -> Self {
        let schema = match Schema::new([Field::required("generate_series", DataType::Int64)]) {
            Ok(schema) => schema,
            Err(err) => {
                tracing::error!(error = %err, "generate_series schema construction failed");
                Schema::empty()
            }
        };
        Self {
            kind: SrfKind::GenerateSeries { start, stop, step },
            schema,
            current: start,
            position: 0,
            cancel_flag: None,
            eof: false,
        }
    }

    /// Construct an `unnest(array)` scan.
    ///
    /// The output schema has a single column named `unnest` with the
    /// array element type.
    #[must_use]
    pub fn unnest(element_type: DataType, elements: Vec<Value>) -> Self {
        let output_type = array_base_type(&element_type).clone();
        let mut flattened = Vec::with_capacity(elements.len());
        flatten_array_elements(&elements, &mut flattened);
        let schema = match Schema::new([Field::required("unnest", output_type)]) {
            Ok(schema) => schema,
            Err(err) => {
                tracing::error!(error = %err, "unnest schema construction failed");
                Schema::empty()
            }
        };
        Self {
            kind: SrfKind::Unnest {
                elements: flattened,
            },
            schema,
            current: 0,
            position: 0,
            cancel_flag: None,
            eof: false,
        }
    }

    /// Attach a query-scoped cancel flag.
    #[must_use]
    pub fn with_cancel_flag(mut self, flag: CancelFlag) -> Self {
        self.cancel_flag = Some(flag);
        self
    }
}

fn array_base_type(ty: &DataType) -> &DataType {
    match ty {
        DataType::Array(inner) => array_base_type(inner),
        other => other,
    }
}

fn flatten_array_elements(elements: &[Value], out: &mut Vec<Value>) {
    for element in elements {
        match element {
            Value::Array { elements, .. } => flatten_array_elements(elements, out),
            other => out.push(other.clone()),
        }
    }
}

impl Operator for FunctionScan {
    #[allow(clippy::similar_names)]
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if let Some(flag) = self.cancel_flag.as_ref()
            && flag.is_set()
        {
            return Err(ExecError::Cancelled);
        }
        if self.eof {
            return Ok(None);
        }

        match &self.kind {
            SrfKind::GenerateSeries { stop, step, .. } => {
                let stop_val = *stop;
                let step_val = *step;

                if step_val == 0 {
                    return Err(ExecError::InvalidParameterValue(
                        "generate_series step size cannot equal zero".to_owned(),
                    ));
                }

                let mut data: Vec<i64> = Vec::with_capacity(BATCH_TARGET_ROWS);
                for _ in 0..BATCH_TARGET_ROWS {
                    // Check bounds: ascending or descending.
                    if (step_val > 0 && self.current > stop_val)
                        || (step_val < 0 && self.current < stop_val)
                    {
                        self.eof = true;
                        break;
                    }
                    data.push(self.current);
                    let Some(next) = self.current.checked_add(step_val) else {
                        self.eof = true;
                        break;
                    };
                    self.current = next;
                }

                if data.is_empty() {
                    self.eof = true;
                    return Ok(None);
                }

                let batch = Batch::new([Column::Int64(NumericColumn::from_data(data))])
                    .map_err(ExecError::from)?;
                Ok(Some(batch))
            }
            SrfKind::Unnest { elements } => {
                if self.position >= elements.len() {
                    self.eof = true;
                    return Ok(None);
                }
                let end = self
                    .position
                    .saturating_add(BATCH_TARGET_ROWS)
                    .min(elements.len());
                let rows: Vec<Vec<Value>> = elements[self.position..end]
                    .iter()
                    .cloned()
                    .map(|value| vec![value])
                    .collect();
                self.position = end;
                if self.position >= elements.len() {
                    self.eof = true;
                }
                build_batch(&rows, &self.schema).map(Some)
            }
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Value};

    use super::FunctionScan;
    use crate::filter_op::batch_to_rows;
    use crate::{CancelFlag, ExecError, Operator};

    fn drain_i64(op: &mut dyn Operator) -> Vec<i64> {
        let schema = op.schema().clone();
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().expect("ok") {
            let rows = batch_to_rows(&b, &schema).expect("decode");
            for row in rows {
                if let Value::Int64(v) = &row[0] {
                    out.push(*v);
                }
            }
        }
        out
    }

    fn first_batch_i64(op: &mut dyn Operator) -> Vec<i64> {
        let schema = op.schema().clone();
        let Some(batch) = op.next_batch().expect("ok") else {
            return Vec::new();
        };
        batch_to_rows(&batch, &schema)
            .expect("decode")
            .into_iter()
            .filter_map(|row| match row.into_iter().next() {
                Some(Value::Int64(value)) => Some(value),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn generate_series_ascending() {
        let mut op = FunctionScan::generate_series(1, 5, 1);
        let vals = drain_i64(&mut op);
        assert_eq!(vals, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn generate_series_descending() {
        let mut op = FunctionScan::generate_series(5, 1, -1);
        let vals = drain_i64(&mut op);
        assert_eq!(vals, vec![5, 4, 3, 2, 1]);
    }

    #[test]
    fn generate_series_zero_step_errors() {
        let mut op = FunctionScan::generate_series(1, 10, 0);
        assert!(matches!(
            op.next_batch(),
            Err(ExecError::InvalidParameterValue(_))
        ));
    }

    #[test]
    fn generate_series_step_two() {
        let mut op = FunctionScan::generate_series(0, 6, 2);
        let vals = drain_i64(&mut op);
        assert_eq!(vals, vec![0, 2, 4, 6]);
    }

    #[test]
    fn generate_series_positive_step_stops_before_overflow() {
        let mut op = FunctionScan::generate_series(i64::MAX - 1, i64::MAX, 2);
        let vals = first_batch_i64(&mut op);
        assert_eq!(vals, vec![i64::MAX - 1]);
    }

    #[test]
    fn generate_series_negative_step_stops_before_overflow() {
        let mut op = FunctionScan::generate_series(i64::MIN + 1, i64::MIN, -2);
        let vals = first_batch_i64(&mut op);
        assert_eq!(vals, vec![i64::MIN + 1]);
    }

    #[test]
    fn generate_series_observes_cancel_flag_before_batch() {
        let flag = CancelFlag::new();
        flag.cancel();
        let mut op = FunctionScan::generate_series(1, 5, 1).with_cancel_flag(flag);
        assert!(matches!(op.next_batch(), Err(ExecError::Cancelled)));
    }

    #[test]
    fn unnest_text_array_emits_values_in_order() {
        let mut op = FunctionScan::unnest(
            DataType::Text { max_len: None },
            vec![Value::Text("red".into()), Value::Text("green".into())],
        );
        let schema = op.schema().clone();
        let mut out = Vec::new();
        while let Some(batch) = op.next_batch().expect("ok") {
            out.extend(crate::filter_op::batch_to_rows(&batch, &schema).expect("decode"));
        }
        assert_eq!(
            out,
            vec![
                vec![Value::Text("red".into())],
                vec![Value::Text("green".into())]
            ]
        );
    }

    #[test]
    fn unnest_multidimensional_array_flattens_values_in_order() {
        let mut op = FunctionScan::unnest(
            DataType::Array(Box::new(DataType::Int32)),
            vec![
                Value::Array {
                    element_type: DataType::Int32,
                    elements: vec![Value::Int32(1), Value::Int32(2)],
                },
                Value::Array {
                    element_type: DataType::Int32,
                    elements: vec![Value::Int32(3), Value::Int32(4)],
                },
            ],
        );
        assert_eq!(op.schema().field_at(0).data_type, DataType::Int32);
        let schema = op.schema().clone();
        let mut out = Vec::new();
        while let Some(batch) = op.next_batch().expect("ok") {
            out.extend(crate::filter_op::batch_to_rows(&batch, &schema).expect("decode"));
        }
        assert_eq!(
            out,
            vec![
                vec![Value::Int32(1)],
                vec![Value::Int32(2)],
                vec![Value::Int32(3)],
                vec![Value::Int32(4)]
            ]
        );
    }
}
