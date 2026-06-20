//! Unit tests for the `session::execute` submodule, split across files.

mod parsers;
mod plan_txn;

use std::sync::Arc;

use super::*;
use tokio::io::duplex;
use ultrasql_core::{Field, Schema};
use ultrasql_planner::{AggregateFunc, LogicalAggregateExpr, LogicalSetVariableAction};

use crate::Server;

fn test_schema() -> Schema {
    Schema::new([
        Field::required("id", DataType::Int32),
        Field::required("value", DataType::Int32),
    ])
    .expect("test schema")
}

fn scan_plan() -> LogicalPlan {
    LogicalPlan::Scan {
        table: "t".to_owned(),
        schema: test_schema(),
        projection: None,
    }
}

fn int_column(name: &str, index: usize) -> ScalarExpr {
    ScalarExpr::Column {
        name: name.to_owned(),
        index,
        data_type: DataType::Int32,
    }
}

/// A `DELETE FROM t` plan in the exact fused-int32-pair shape that
/// [`Session::fast_dml_checks_cacheable`] accepts, so it is eligible
/// for the precheck cache.
fn cacheable_delete_plan() -> LogicalPlan {
    LogicalPlan::Delete {
        table: "t".to_owned(),
        input: Box::new(scan_plan()),
        returning: Vec::new(),
        schema: Schema::empty(),
    }
}

fn test_session() -> Session<tokio::io::DuplexStream> {
    let (io, _peer) = duplex(64);
    Session::new(io, Arc::new(Server::with_sample_database()), None)
}

fn first_data_row_text(result: &SelectResult) -> String {
    let Some(BackendMessage::DataRow { columns }) = result
        .messages
        .iter()
        .find(|msg| matches!(msg, BackendMessage::DataRow { .. }))
    else {
        panic!("missing data row");
    };
    String::from_utf8(columns[0].clone().expect("value")).expect("utf8")
}
