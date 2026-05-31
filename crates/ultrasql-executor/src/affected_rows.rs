use ultrasql_vec::Batch;
use ultrasql_vec::column::{Column, NumericColumn};

use crate::ExecError;

pub(crate) fn affected_rows_count(rows: usize, context: &str) -> Result<i64, ExecError> {
    i64::try_from(rows).map_err(|_| {
        ExecError::NumericFieldOverflow(format!("{context} affected row count overflow"))
    })
}

pub(crate) fn affected_rows_batch(rows: usize, context: &str) -> Result<Batch, ExecError> {
    let affected = affected_rows_count(rows, context)?;
    Batch::new([Column::Int64(NumericColumn::from_data(vec![affected]))]).map_err(ExecError::from)
}

#[cfg(test)]
mod tests {
    use super::affected_rows_count;
    use crate::ExecError;

    #[test]
    fn affected_rows_count_allows_i64_max_boundary() {
        let max_rows = usize::try_from(i64::MAX).expect("test host supports i64::MAX usize");
        let count = affected_rows_count(max_rows, "fused DML").expect("boundary count");
        assert_eq!(count, i64::MAX);
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn affected_rows_count_rejects_i64_overflow() {
        let overflowing = usize::try_from(i64::MAX).expect("test host supports i64::MAX usize") + 1;
        let err = affected_rows_count(overflowing, "fused DML")
            .expect_err("affected row count overflow must not clamp");
        assert!(matches!(err, ExecError::NumericFieldOverflow(_)), "{err:?}");
    }
}
