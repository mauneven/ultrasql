//! Set-returning function (SRF) scan operator.
//!
//! [`FunctionScan`] evaluates a set-returning function and emits its rows
//! as batches.
//!
//! # `generate_series`
//!
//! `generate_series(start, stop, step)` emits integer rows from `start` to
//! `stop` (inclusive) stepping by `step`. Negative `step` allows
//! descending series. A zero `step` returns no rows (matching PostgreSQL).
//!
//! The output schema has a single `Int64` column named `generate_series`.
//!
use ultrasql_core::{DataType, Field, Schema, Value};
use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn};

use crate::seq_scan::build_batch;
use crate::{ExecError, Operator};

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
    eof: bool,
}

impl FunctionScan {
    /// Construct a `generate_series(start, stop, step)` scan.
    ///
    /// The output schema has a single `Int64` column named `generate_series`.
    #[must_use]
    #[allow(clippy::similar_names)] // `stop` and `step` are standard generate_series parameter names
    pub fn generate_series(start: i64, stop: i64, step: i64) -> Self {
        let schema = Schema::new([Field::required("generate_series", DataType::Int64)])
            .expect("schema is well-formed");
        Self {
            kind: SrfKind::GenerateSeries { start, stop, step },
            schema,
            current: start,
            position: 0,
            eof: step == 0, // zero step immediately exhausted
        }
    }

    /// Construct an `unnest(array)` scan.
    ///
    /// The output schema has a single column named `unnest` with the
    /// array element type.
    #[must_use]
    pub fn unnest(element_type: DataType, elements: Vec<Value>) -> Self {
        let schema =
            Schema::new([Field::required("unnest", element_type)]).expect("schema is well-formed");
        Self {
            kind: SrfKind::Unnest { elements },
            schema,
            current: 0,
            position: 0,
            eof: false,
        }
    }
}

impl Operator for FunctionScan {
    #[allow(clippy::similar_names)]
    fn next_batch(&mut self) -> Result<Option<Batch>, ExecError> {
        if self.eof {
            return Ok(None);
        }

        match &self.kind {
            SrfKind::GenerateSeries { stop, step, .. } => {
                let stop_val = *stop;
                let step_val = *step;

                if step_val == 0 {
                    self.eof = true;
                    return Ok(None);
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
                    self.current = self.current.wrapping_add(step_val);
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
    use crate::Operator;
    use crate::filter_op::batch_to_rows;

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
    fn generate_series_zero_step_emits_nothing() {
        let mut op = FunctionScan::generate_series(1, 10, 0);
        assert!(op.next_batch().expect("ok").is_none());
    }

    #[test]
    fn generate_series_step_two() {
        let mut op = FunctionScan::generate_series(0, 6, 2);
        let vals = drain_i64(&mut op);
        assert_eq!(vals, vec![0, 2, 4, 6]);
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
}
