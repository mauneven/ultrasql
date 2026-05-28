//! Typed representations of PostgreSQL wire-protocol v3 messages.
//!
//! The two top-level enums [`FrontendMessage`] and [`BackendMessage`]
//! mirror the message catalog from the PostgreSQL protocol
//! documentation, "Message Formats" chapter. Each variant carries the
//! semantic payload after the wire framing (1-byte type tag and 4-byte
//! length) has been stripped. The framing itself is performed by the
//! codec functions in [`crate::codec`].
//!
//! ## Strings on the wire
//!
//! Every textual field that PostgreSQL transmits as a NUL-terminated C
//! string is represented here as a Rust `String`. The codec rejects
//! non-UTF-8 byte sequences at decode time so consumers never need to
//! handle invalid UTF-8 manually. UltraSQL treats `client_encoding`
//! values other than UTF-8 as unsupported and refuses them in the
//! server crate; this design keeps the protocol layer honest.
//!
//! ## Endianness
//!
//! PostgreSQL's wire protocol is big-endian for every multi-byte
//! integer. That convention is local to this crate; the rest of
//! UltraSQL uses little-endian on disk. The codec is responsible for
//! the byte-order translation so the typed messages exposed here look
//! like ordinary host-endian values.

/// Selector byte used in [`FrontendMessage::Describe`] and
/// [`FrontendMessage::Close`] to distinguish between a named prepared
/// statement and a named portal.
///
/// PostgreSQL wire spec: the byte `b'S'` means "statement";
/// the byte `b'P'` means "portal". These are the only two legal values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DescribeKind {
    /// Describe or close a named prepared statement (`b'S'`).
    Statement,
    /// Describe or close a named portal (`b'P'`).
    Portal,
}

/// Per-column field description in a [`BackendMessage::RowDescription`].
///
/// All offsets and OIDs follow the wire contract. `type_size` is
/// negative for variable-length types (e.g. `text`, `bytea`); positive
/// values denote a fixed byte width. `format_code` is `0` for the text
/// format and `1` for the binary format — the only two values defined
/// by the v3 protocol.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FieldDescription {
    /// Column name as it should appear to the client.
    pub name: String,
    /// OID of the table the column belongs to, or `0` if the column is
    /// not a simple reference to a base table (expressions, joins).
    pub table_oid: u32,
    /// Attribute number of the column within its table, or `0` for
    /// non-table columns. PostgreSQL uses signed `i16` here.
    pub col_attnum: i16,
    /// OID of the column's data type. Stable across releases.
    pub type_oid: u32,
    /// Byte width of the data type. Negative values denote a
    /// variable-length type; `-1` is the canonical "variable" marker.
    pub type_size: i16,
    /// Type-specific modifier (e.g. precision for `numeric`). `-1`
    /// means "no modifier".
    pub type_modifier: i32,
    /// `0` for text format, `1` for binary format.
    pub format_code: i16,
}

/// Messages the client sends to the server.
///
/// The variant name matches the canonical PostgreSQL name; the
/// comment on each variant gives the 1-byte type tag used on the
/// wire. [`FrontendMessage::StartupMessage`] is the lone exception:
/// it carries no type byte, only the length prefix and payload.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum FrontendMessage {
    /// Initial handshake. Sent exactly once at the start of a
    /// connection, before any other frontend message. The protocol
    /// number is two `u16`s on the wire (major, minor) — protocol v3
    /// is `(3, 0)`.
    StartupMessage {
        /// Major version (`3` for v3).
        protocol_major: u16,
        /// Minor version (`0` for v3.0).
        protocol_minor: u16,
        /// Connection parameters: `(name, value)` pairs in order. The
        /// list is empty-terminated on the wire with a single NUL byte;
        /// that terminator is consumed by the decoder.
        params: Vec<(String, String)>,
    },

    /// Simple query (`'Q'`): a single SQL string the server is expected
    /// to parse, execute, and respond to with one or more results.
    Query {
        /// SQL text. The server splits multi-statement queries on
        /// semicolons.
        sql: String,
    },

    /// Parse (`'P'`): prepare a SQL statement under an optional name.
    Parse {
        /// Statement name; an empty string designates the unnamed
        /// statement.
        name: String,
        /// SQL text being prepared.
        sql: String,
        /// Caller-specified parameter type OIDs. An empty list lets
        /// the server infer types.
        param_types: Vec<u32>,
    },

    /// Bind (`'B'`): create a portal from a prepared statement,
    /// supplying parameter values and requesting per-column result
    /// formats.
    ///
    /// `params` carries the parameter values; `None` represents a SQL
    /// `NULL` (the PostgreSQL protocol encodes this as length `-1`).
    Bind {
        /// Portal name; empty for the unnamed portal.
        portal_name: String,
        /// Source statement name; empty for the unnamed statement.
        statement_name: String,
        /// Per-parameter format codes (`0` = text, `1` = binary). Three
        /// conventions are spec-defined:
        ///
        /// - empty vector → every parameter is in text format,
        /// - single element → that single code applies to every
        ///   parameter (the libpq "all-same" shortcut),
        /// - one element per parameter → element `i` governs `params[i]`.
        ///
        /// The decoder preserves the raw vector verbatim; callers
        /// resolve the convention via the rules above. Earlier
        /// versions of this enum discarded this field; preserving it
        /// is required for binary-format clients (e.g. `tokio-postgres`
        /// in its default prepared-statement path).
        param_formats: Vec<i16>,
        /// Parameter values, in declaration order.
        params: Vec<Option<Vec<u8>>>,
        /// Per-column result format codes (`0` = text, `1` = binary).
        /// Same three conventions as `param_formats`.
        result_formats: Vec<i16>,
    },

    /// Describe (`'D'`): request metadata about a portal or statement.
    Describe {
        /// Whether to describe a prepared statement or a portal.
        kind: DescribeKind,
        /// Name of the target portal or statement.
        name: String,
    },

    /// Execute (`'E'`): run a previously-bound portal.
    Execute {
        /// Portal name; empty for the unnamed portal.
        portal: String,
        /// Maximum number of rows to return, or `0` for no limit.
        max_rows: i32,
    },

    /// Cancel request: an out-of-band connection that asks the server
    /// to flag a sibling session for cancellation.
    ///
    /// On the wire this looks like a startup packet — there is no type
    /// tag — but the first four bytes of the payload are the magic
    /// code `80877102` (`(1234 << 16) | 5678`) instead of a protocol
    /// version. The remaining 8 bytes are the target session's
    /// `(process_id, secret_key)` as reported earlier in its
    /// [`BackendMessage::BackendKeyData`]. The server uses the
    /// `(process_id, secret_key)` pair to look up the target session
    /// in its `CancelRegistry` and flip the per-query `CancelFlag`;
    /// the cancel-bearing connection is closed without further
    /// dialogue.
    CancelRequest {
        /// Target session's process id.
        process_id: i32,
        /// Target session's secret key.
        secret_key: i32,
    },

    /// Sync (`'S'`): close the current pipeline, causing the server to
    /// flush any pending [`BackendMessage::ReadyForQuery`].
    Sync,

    /// Terminate (`'X'`): polite goodbye; the server should close the
    /// connection after acknowledging.
    Terminate,

    /// Password (`'p'`): payload of the cleartext or MD5-hashed
    /// password during authentication.
    Password {
        /// Password bytes (cleartext or hashed, depending on the
        /// authentication request the server sent).
        password: String,
    },

    /// Close (`'C'`): destroy a named prepared statement or portal.
    ///
    /// After a `Close` the server replies with
    /// [`BackendMessage::CloseComplete`] (or [`BackendMessage::ErrorResponse`]
    /// if the named object does not exist and the server chooses to
    /// surface the error).
    Close {
        /// Whether to close a prepared statement or a portal.
        kind: DescribeKind,
        /// Name of the statement or portal to close; empty string names
        /// the unnamed object.
        name: String,
    },

    /// Flush (`'H'`): request that the server flush any pending output.
    ///
    /// The server sends all queued outgoing messages without waiting for
    /// the next `Sync`. Flush does not change the transaction state.
    Flush,

    /// `CopyData` (`'d'`): a block of data in a COPY stream.
    ///
    /// In a COPY FROM STDIN flow the client sends one or more
    /// `CopyData` messages followed by a [`Self::CopyDone`] or
    /// [`Self::CopyFail`]. The payload is opaque bytes; their meaning
    /// is determined by the COPY format negotiated earlier.
    CopyData(Vec<u8>),

    /// `CopyDone` (`'c'`): signals the end of a successful COPY FROM STDIN stream.
    CopyDone,

    /// `CopyFail` (`'f'`): signals that the client-side COPY FROM STDIN
    /// operation failed.
    ///
    /// The string is a human-readable error message. The server will
    /// abort the COPY and return an `ErrorResponse`.
    CopyFail(String),

    /// `FunctionCall` (`'F'`): invoke a server-side function via the
    /// legacy function-call sub-protocol.
    ///
    /// UltraSQL decodes the message tag and payload length but does not
    /// implement the sub-protocol. The dispatcher returns
    /// `ErrorResponse` with `SQLSTATE 0A000` (feature not supported)
    /// when this variant is received.
    FunctionCall {
        /// OID of the function to call.
        function_oid: u32,
        /// Per-argument format codes (`0` = text, `1` = binary).
        arg_formats: Vec<u16>,
        /// Argument values; `None` is SQL NULL.
        args: Vec<Option<Vec<u8>>>,
        /// Result format code (`0` = text, `1` = binary).
        result_format: u16,
    },
}

/// Messages the server sends to the client.
///
/// As with [`FrontendMessage`], the variant name matches the canonical
/// PostgreSQL name and the doc comment lists the 1-byte type tag.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum BackendMessage {
    /// `AuthenticationOk` (`'R'`, sub-tag `0`): the server has
    /// accepted the authentication exchange.
    AuthenticationOk,

    /// `AuthenticationCleartextPassword` (`'R'`, sub-tag `3`): the
    /// server requests a cleartext password.
    AuthenticationCleartextPassword,

    /// `AuthenticationMD5Password` (`'R'`, sub-tag `5`): the server
    /// requests an MD5-hashed password using the supplied salt.
    AuthenticationMD5Password {
        /// 4-byte salt to mix into the MD5 hash.
        salt: [u8; 4],
    },

    /// `ParameterStatus` (`'S'`): announces the server's value for a
    /// runtime parameter (`server_version`, `client_encoding`, etc.).
    ParameterStatus {
        /// Parameter name.
        name: String,
        /// Parameter value as a string.
        value: String,
    },

    /// `BackendKeyData` (`'K'`): cancellation handle for this
    /// connection.
    BackendKeyData {
        /// Server-assigned process identifier.
        process_id: i32,
        /// Secret key the client must echo in a cancel request.
        secret_key: i32,
    },

    /// `ReadyForQuery` (`'Z'`): signals that the server is ready for
    /// the next request and reports the current transaction state.
    ReadyForQuery {
        /// `b'I'` for idle, `b'T'` for inside a transaction block, or
        /// `b'E'` for inside a failed transaction block.
        status: u8,
    },

    /// `RowDescription` (`'T'`): per-column metadata for the rows
    /// that follow.
    RowDescription {
        /// Field descriptions, in column order.
        fields: Vec<FieldDescription>,
    },

    /// `DataRow` (`'D'`): values for a single row. `None` encodes SQL
    /// `NULL`; otherwise the bytes are the column value in the format
    /// announced by the preceding [`BackendMessage::RowDescription`].
    DataRow {
        /// Column values, in column order.
        columns: Vec<Option<Vec<u8>>>,
    },

    /// `CommandComplete` (`'C'`): the in-progress command finished.
    /// The `tag` is a short PostgreSQL command string such as
    /// `"SELECT 12"` or `"INSERT 0 3"`.
    CommandComplete {
        /// Completion tag string.
        tag: String,
    },

    /// `ErrorResponse` (`'E'`): the in-progress command failed. The
    /// payload carries one or more fields, each preceded by a 1-byte
    /// field-type code. The list is empty-terminated on the wire.
    ErrorResponse {
        /// `(field_type, value)` pairs in transmission order.
        fields: Vec<(u8, String)>,
    },

    /// `EmptyQueryResponse` (`'I'`): the previous query string was
    /// effectively empty.
    EmptyQueryResponse,

    /// `NoticeResponse` (`'N'`): the server is sending an
    /// informational notice that did not abort the command. Field
    /// layout matches [`BackendMessage::ErrorResponse`].
    NoticeResponse {
        /// `(field_type, value)` pairs in transmission order.
        fields: Vec<(u8, String)>,
    },

    /// `ParseComplete` (`'1'`): the `Parse` step of the Extended Query
    /// protocol succeeded and the statement is ready to be bound.
    ParseComplete,

    /// `BindComplete` (`'2'`): the `Bind` step of the Extended Query
    /// protocol succeeded and the portal is ready to be executed.
    BindComplete,

    /// `CloseComplete` (`'3'`): a `Close` message was processed
    /// successfully.
    CloseComplete,

    /// `NoData` (`'n'`): sent in response to a `Describe` when the
    /// statement or portal produces no row data (e.g. `INSERT`, `UPDATE`,
    /// `DELETE`, `SET`, `CREATE TABLE`, …).
    NoData,

    /// `ParameterDescription` (`'t'`): sent in response to a `Describe`
    /// of a prepared statement. Lists the OIDs of the statement's
    /// parameter types in declaration order.
    ParameterDescription {
        /// Parameter type OIDs, in declaration order. An empty vector
        /// means the statement has no parameters.
        type_oids: Vec<u32>,
    },

    /// `PortalSuspended` (`'s'`): returned by `Execute` when the
    /// portal's row limit was reached before the query ran to
    /// completion. The portal remains open; a subsequent `Execute` will
    /// resume from where it was suspended.
    PortalSuspended,

    /// `CopyInResponse` (`'G'`): the server is requesting a
    /// COPY FROM STDIN data stream from the client.
    CopyInResponse {
        /// Overall copy format: `0` = text, `1` = binary.
        overall_format: u8,
        /// Per-column format codes (`0` = text, `1` = binary).
        column_formats: Vec<u16>,
    },

    /// `CopyOutResponse` (`'H'`): the server is initiating a
    /// COPY TO STDOUT data stream to the client.
    CopyOutResponse {
        /// Overall copy format: `0` = text, `1` = binary.
        overall_format: u8,
        /// Per-column format codes (`0` = text, `1` = binary).
        column_formats: Vec<u16>,
    },

    /// `CopyData` (`'d'`): a block of data in a COPY stream from the
    /// server to the client. Follows a [`Self::CopyOutResponse`]; ends
    /// with a [`Self::CopyDone`] or [`Self::ErrorResponse`].
    CopyData(Vec<u8>),

    /// `CopyDone` (`'c'`): signals the end of a COPY stream from the
    /// server to the client.
    CopyDone,

    /// `NotificationResponse` (`'A'`): async pub-sub message delivered
    /// to every connection that has subscribed to `channel` via
    /// `LISTEN`. The wire layout is `Int32 process_id`, `CString channel`,
    /// `CString payload`; an empty payload is a valid PostgreSQL value
    /// and round-trips as an empty string.
    NotificationResponse {
        /// Process identifier of the backend that emitted the `NOTIFY`.
        process_id: i32,
        /// Channel name as provided to `LISTEN` / `NOTIFY`.
        channel: String,
        /// Payload string. The empty string is a valid payload and is
        /// distinguishable on the wire only by the absence of an
        /// explicit literal in the original `NOTIFY` — both surfaces
        /// share the same empty-string representation here.
        payload: String,
    },
}
