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

/// Per-column field description in a [`BackendMessage::RowDescription`].
///
/// All offsets and OIDs are PostgreSQL-compatible. `type_size` is
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
        /// Parameter values, in declaration order. UltraSQL exposes a
        /// simplified Bind that omits the per-parameter format-code
        /// arrays; callers that need binary parameters should use
        /// [`Self::Parse`] with explicit type OIDs and supply binary
        /// payloads through the value bytes.
        params: Vec<Option<Vec<u8>>>,
        /// Per-column result format codes (`0` = text, `1` = binary).
        /// An empty vector means "default to text for every column".
        result_formats: Vec<i16>,
    },

    /// Describe (`'D'`): request metadata about a portal or statement.
    Describe {
        /// `b'S'` to describe a statement, `b'P'` to describe a portal.
        kind: u8,
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
}
