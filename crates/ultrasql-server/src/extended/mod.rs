//! PostgreSQL Extended Query Protocol server-side dispatch.
//!
//! The Simple Query protocol carries one `Query` message at a time and
//! parses/binds/executes it inline. The Extended Query protocol splits
//! the same work across five client messages:
//!
//! ```text
//! Parse(name, sql, oids)        → ParseComplete
//! Bind (portal, stmt, params)   → BindComplete
//! Describe(S|P, name)           → ParameterDescription? RowDescription | NoData
//! Execute(portal, max_rows)     → DataRow* (CommandComplete | PortalSuspended)
//! Sync                          → ReadyForQuery
//! Close(S|P, name)              → CloseComplete
//! Flush                         → (no response, just flush buffered output)
//! ```
//!
//! ## Per-connection state
//!
//! Two `HashMap`s store named statements and named portals. They are
//! owned by the `Session` struct in `lib.rs` and accessed only by the
//! connection's own task, so no synchronisation primitive is needed
//! (per AGENTS.md §5: "default to the simplest primitive that meets the
//! workload" — the workload here is single-threaded). The empty string
//! is the canonical "unnamed" key, per the protocol spec.
//!
//! ## Parameter substitution strategy
//!
//! Bind decodes each parameter value (per its format code and the
//! statement's declared type OID) into a `Value`, then walks the
//! prepared statement's bound [`LogicalPlan`] and rewrites every
//! `ScalarExpr::Parameter` into a `ScalarExpr::Literal` of the
//! corresponding value. The substituted plan is stored in the portal
//! and executed exactly the same way as Simple Query plans.
//!
//! The tradeoff: parameters do not flow through the optimizer with a
//! "parameter" identity, so plan caching does not yet share a single
//! generic plan across multiple bindings. That is acceptable for v0.5
//! (each Bind re-parses cheaply). The alternative — keeping the
//! `Parameter` node and plumbing a bound parameter vector through every
//! operator — would require touching `Filter`, `Project`, `HashAggregate`,
//! and the `Eval` constructors at the `lower_query` level. Substitution is
//! a self-contained rewrite that touches no executor code.
//!
//! ## Error handling
//!
//! Per the Extended Query spec, once any pipeline message produces an
//! error, the server replies with `ErrorResponse` and then **skips every
//! subsequent extended-protocol message until it sees a `Sync`**. Only
//! after `Sync` does it emit `ReadyForQuery` and resume processing.
//! [`ExtendedConnState::pipeline_failed`] tracks this skip state.

use std::collections::HashMap;

use ultrasql_planner::LogicalPlan;
use ultrasql_protocol::BackendMessage;

// ---------------------------------------------------------------------------
// Type-OID constants. Duplicated narrowly with `result_encoder.rs` so this
// module is self-contained for the binary-format param decoder.
// ---------------------------------------------------------------------------

/// PostgreSQL type OID for `bool`. Pulled from `pg_type.dat`.
const PG_OID_BOOL: u32 = 16;
/// PostgreSQL type OID for `int2`.
const PG_OID_INT2: u32 = 21;
/// PostgreSQL type OID for `int4`.
const PG_OID_INT4: u32 = 23;
/// PostgreSQL type OID for `int8`.
const PG_OID_INT8: u32 = 20;
/// PostgreSQL type OID for `float4`.
const PG_OID_FLOAT4: u32 = 700;
/// PostgreSQL type OID for `float8`.
const PG_OID_FLOAT8: u32 = 701;
/// PostgreSQL type OID for `numeric`.
const PG_OID_NUMERIC: u32 = 1700;
/// PostgreSQL type OID for `money`.
const PG_OID_MONEY: u32 = 790;
/// PostgreSQL type OID for `text`.
const PG_OID_TEXT: u32 = 25;
/// PostgreSQL type OID for `bytea`.
const PG_OID_BYTEA: u32 = 17;
/// PostgreSQL type OID for `json`.
const PG_OID_JSON: u32 = 114;
/// PostgreSQL type OID for `jsonb`.
const PG_OID_JSONB: u32 = 3802;
/// PostgreSQL type OID for `xml`.
const PG_OID_XML: u32 = 142;
/// PostgreSQL type OID for `tsvector`.
const PG_OID_TSVECTOR: u32 = 3614;
/// PostgreSQL type OID for `tsquery`.
const PG_OID_TSQUERY: u32 = 3615;
/// PostgreSQL type OID for `varchar`.
const PG_OID_VARCHAR: u32 = 1043;
/// PostgreSQL type OID for `bpchar` (`char(n)`).
const PG_OID_BPCHAR: u32 = 1042;
/// PostgreSQL type OID for `bit`.
const PG_OID_BIT: u32 = 1560;
/// PostgreSQL type OID for `varbit`.
const PG_OID_VARBIT: u32 = 1562;
/// PostgreSQL type OID for `cidr`.
const PG_OID_CIDR: u32 = 650;
/// PostgreSQL type OID for `inet`.
const PG_OID_INET: u32 = 869;
/// PostgreSQL type OID for `macaddr`.
const PG_OID_MACADDR: u32 = 829;
/// PostgreSQL type OID for `macaddr8`.
const PG_OID_MACADDR8: u32 = 774;
/// PostgreSQL type OID for `date`.
const PG_OID_DATE: u32 = 1082;
/// PostgreSQL type OID for `time`.
const PG_OID_TIME: u32 = 1083;
/// PostgreSQL type OID for `timestamp`.
const PG_OID_TIMESTAMP: u32 = 1114;
/// PostgreSQL type OID for `timetz`.
const PG_OID_TIMETZ: u32 = 1266;
/// PostgreSQL type OID for `timestamptz`.
const PG_OID_TIMESTAMPTZ: u32 = 1184;
/// PostgreSQL type OID for `oid`.
const PG_OID_OID: u32 = 26;
/// PostgreSQL type OID for `regclass`.
const PG_OID_REGCLASS: u32 = 2205;
/// PostgreSQL type OID for `regtype`.
const PG_OID_REGTYPE: u32 = 2206;
/// PostgreSQL type OID for `pg_lsn`.
const PG_OID_PG_LSN: u32 = 3220;
/// PostgreSQL type OID for `uuid`.
const PG_OID_UUID: u32 = 2950;
/// PostgreSQL type OID for `bool[]`.
const PG_OID_BOOL_ARRAY: u32 = 1000;
/// PostgreSQL type OID for `int2[]`.
const PG_OID_INT2_ARRAY: u32 = 1005;
/// PostgreSQL type OID for `int4[]`.
const PG_OID_INT4_ARRAY: u32 = 1007;
/// PostgreSQL type OID for `int8[]`.
const PG_OID_INT8_ARRAY: u32 = 1016;
/// PostgreSQL type OID for `float4[]`.
const PG_OID_FLOAT4_ARRAY: u32 = 1021;
/// PostgreSQL type OID for `float8[]`.
const PG_OID_FLOAT8_ARRAY: u32 = 1022;
/// PostgreSQL type OID for `numeric[]`.
const PG_OID_NUMERIC_ARRAY: u32 = 1231;
/// PostgreSQL type OID for `money[]`.
const PG_OID_MONEY_ARRAY: u32 = 791;
/// PostgreSQL type OID for `text[]`.
const PG_OID_TEXT_ARRAY: u32 = 1009;
/// PostgreSQL type OID for `bpchar[]`.
const PG_OID_BPCHAR_ARRAY: u32 = 1014;
/// PostgreSQL type OID for `bit[]`.
const PG_OID_BIT_ARRAY: u32 = 1561;
/// PostgreSQL type OID for `varbit[]`.
const PG_OID_VARBIT_ARRAY: u32 = 1563;
/// PostgreSQL type OID for `cidr[]`.
const PG_OID_CIDR_ARRAY: u32 = 651;
/// PostgreSQL type OID for `inet[]`.
const PG_OID_INET_ARRAY: u32 = 1041;
/// PostgreSQL type OID for `macaddr[]`.
const PG_OID_MACADDR_ARRAY: u32 = 1040;
/// PostgreSQL type OID for `macaddr8[]`.
const PG_OID_MACADDR8_ARRAY: u32 = 775;
/// PostgreSQL type OID for `bytea[]`.
const PG_OID_BYTEA_ARRAY: u32 = 1001;
/// PostgreSQL type OID for `uuid[]`.
const PG_OID_UUID_ARRAY: u32 = 2951;
/// PostgreSQL type OID for `json[]`.
const PG_OID_JSON_ARRAY: u32 = 199;
/// PostgreSQL type OID for `jsonb[]`.
const PG_OID_JSONB_ARRAY: u32 = 3807;
/// PostgreSQL type OID for `xml[]`.
const PG_OID_XML_ARRAY: u32 = 143;
/// PostgreSQL type OID for `tsvector[]`.
const PG_OID_TSVECTOR_ARRAY: u32 = 3643;
/// PostgreSQL type OID for `tsquery[]`.
const PG_OID_TSQUERY_ARRAY: u32 = 3645;
/// PostgreSQL type OID for `date[]`.
const PG_OID_DATE_ARRAY: u32 = 1182;
/// PostgreSQL type OID for `time[]`.
const PG_OID_TIME_ARRAY: u32 = 1183;
/// PostgreSQL type OID for `timestamp[]`.
const PG_OID_TIMESTAMP_ARRAY: u32 = 1115;
/// PostgreSQL type OID for `timetz[]`.
const PG_OID_TIMETZ_ARRAY: u32 = 1270;
/// PostgreSQL type OID for `timestamptz[]`.
const PG_OID_TIMESTAMPTZ_ARRAY: u32 = 1185;
/// PostgreSQL type OID for `oid[]`.
const PG_OID_OID_ARRAY: u32 = 1028;
/// PostgreSQL type OID for `regclass[]`.
const PG_OID_REGCLASS_ARRAY: u32 = 2210;
/// PostgreSQL type OID for `regtype[]`.
const PG_OID_REGTYPE_ARRAY: u32 = 2211;
/// PostgreSQL type OID for `pg_lsn[]`.
const PG_OID_PG_LSN_ARRAY: u32 = 3221;

mod codec;
mod execute;
mod handlers;
mod params;
mod substitute;

#[cfg(test)]
mod tests;

pub use execute::execute_portal;
pub use handlers::{
    handle_bind, handle_close, handle_describe_portal, handle_describe_statement, handle_parse,
};
pub(crate) use substitute::substitute_parameters_in_plan;

// ---------------------------------------------------------------------------
// Cached, parsed-and-bound prepared statement.
// ---------------------------------------------------------------------------

/// A `Parse`d statement waiting for `Bind`.
///
/// `plan` is `None` for empty statements (those parse and produce no
/// AST). `param_type_oids` retains the OIDs the client supplied; the
/// server uses them to decode binary parameters in `Bind`. `n_params`
/// is the maximum `$N` index referenced in the bound plan, computed at
/// Parse time so `Bind` can validate parameter count.
#[derive(Clone, Debug)]
pub struct PreparedStatement {
    /// Raw SQL text retained for diagnostics.
    pub sql: String,
    /// Bound logical plan. `None` for an empty statement (SQL `""`).
    pub plan: Option<LogicalPlan>,
    /// Stable hash of the bound plan before bind-value substitution.
    pub plan_hash: u64,
    /// Parameter type OIDs as declared by the client. May be shorter
    /// than `n_params` (the client is allowed to leave types unset).
    pub param_type_oids: Vec<u32>,
    /// Number of distinct `$N` placeholder slots referenced in `plan`.
    /// Equal to the highest `index` seen; `$1`+`$3` yields `n_params=3`.
    pub n_params: u32,
    /// `$N` slots used only by LIMIT/OFFSET. These are rebound at Bind
    /// time because the logical plan stores row caps as integers.
    pub limit_offset_param_indexes: Vec<u32>,
}

/// A bound portal: a prepared statement plus the parameter values
/// substituted into its plan, plus the per-result-column format codes.
#[derive(Clone, Debug)]
pub struct BoundPortal {
    /// The plan with `Parameter` nodes already replaced by `Literal`s.
    pub plan: Option<LogicalPlan>,
    /// Raw prepared SQL text with `$N` placeholders intact.
    pub sql: String,
    /// Stable hash of the prepared plan before bind-value substitution.
    pub plan_hash: u64,
    /// Number of bind parameters supplied by the client.
    pub bind_param_count: u32,
    /// Whether concrete bind values are redacted from workload records.
    pub bind_params_redacted: bool,
    /// Per-result-column format codes (`0` = text, `1` = binary).
    ///
    /// Spec conventions: empty → all text; single element → applies to
    /// every column; one-per → element `i` governs result column `i`.
    pub result_formats: Vec<i16>,
}

/// Per-connection Extended Query state.
///
/// One instance per `Session`. Owned by the session, accessed only by
/// the connection's task, so no synchronisation primitive is needed.
///
/// `pipeline_failed` implements the spec's "ignore everything until
/// Sync" rule: once any extended-protocol message produces an error,
/// subsequent Parse/Bind/Describe/Execute/Close messages are skipped
/// silently until a `Sync` resets the flag.
#[derive(Default)]
pub struct ExtendedConnState {
    /// Prepared statements, keyed by name. Empty string = unnamed.
    pub statements: HashMap<String, PreparedStatement>,
    /// Open portals, keyed by name. Empty string = unnamed.
    pub portals: HashMap<String, BoundPortal>,
    /// Suspended portals, keyed by name. Populated on `PortalSuspended`
    /// and consumed by the next `Execute` against the same portal — see
    /// §1.10 portal-resumption work and `execute_portal`.
    pub suspended: HashMap<String, SuspendedPortal>,
    /// `true` after an error in the current pipeline; cleared by `Sync`.
    pub pipeline_failed: bool,
}

impl std::fmt::Debug for ExtendedConnState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExtendedConnState")
            .field("statements", &self.statements)
            .field("portals", &self.portals)
            .field("suspended", &self.suspended.keys().collect::<Vec<_>>())
            .field("pipeline_failed", &self.pipeline_failed)
            .finish()
    }
}

/// State retained across a `PortalSuspended` boundary so the next
/// `Execute` on the same portal can resume from where the previous one
/// stopped instead of re-running the plan from scratch.
#[derive(Debug)]
pub struct SuspendedPortal {
    /// The operator stream still hot — `next_batch` returns the rows
    /// that have not yet been emitted to the client.
    pub op: Box<dyn ultrasql_executor::Operator>,
    /// Partially-consumed batch from the last `next_batch` call, and the
    /// index of the first row not yet emitted. `None` means the boundary
    /// landed cleanly between batches.
    pub leftover: Option<(ultrasql_vec::Batch, usize)>,
    /// Total rows emitted so far across every resumption. Returned to
    /// the client in the final `CommandComplete "SELECT n"` tag.
    pub emitted: u64,
    /// Per-result-column format codes carried over from the original
    /// Bind so the resumed Execute encodes columns identically.
    pub result_formats: Vec<i16>,
    /// Text display settings captured from the session at first Execute.
    pub(crate) text_options: crate::result_encoder::TextEncodingOptions,
}

impl ExtendedConnState {
    /// Build an empty state container.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark the current pipeline as failed; subsequent extended-protocol
    /// messages (other than `Sync`) are ignored until `Sync` resets.
    pub const fn mark_failed(&mut self) {
        self.pipeline_failed = true;
    }

    /// Called by `Sync`: clear the failure flag.
    pub const fn reset_on_sync(&mut self) {
        self.pipeline_failed = false;
    }
}

// ---------------------------------------------------------------------------
// Execute.
// ---------------------------------------------------------------------------

/// Outcome of [`execute_portal`].
///
/// `messages` is the ordered list of `DataRow` / `CommandComplete` (or
/// `PortalSuspended`) messages the caller must emit. For a SELECT,
/// `RowDescription` is **not** included — the caller emits it ahead of
/// time when the client sent a `Describe`, or omits it entirely when
/// the client didn't (some drivers skip `Describe` for already-described
/// portals).
#[derive(Debug)]
pub struct ExecuteOutcome {
    /// The backend messages to send, in order.
    pub messages: Vec<BackendMessage>,
}
