//! Catalog row types — `TableEntry` and `IndexEntry`.
//!
//! These are the canonical in-memory descriptions of a relation and an
//! index. They are kept deliberately small and `Clone`-friendly so the
//! catalog can hand callers an owned snapshot without lifetimes leaking
//! across thread boundaries.
//!
//! # On-disk parity
//!
//! Each field maps to a column on the future system catalog tables:
//!
//! | Field                 | Future column                |
//! |-----------------------|------------------------------|
//! | `oid`                 | `pg_class.oid`               |
//! | `name`                | `pg_class.relname`           |
//! | `schema_name`         | `pg_namespace.nspname`       |
//! | `schema`              | derived from `pg_attribute`  |
//! | `created_at_lsn`      | `pg_class.relfilelsn` (new)  |
//! | `n_blocks`            | `pg_class.relpages`          |
//! | `root_block`          | `pg_class.relfilenode` (new) |
//!
//! For [`IndexEntry`] the parity is with `pg_index`. The mapping is
//! noted here so the persistent implementation can be slotted in by a
//! follow-up RFC without renaming fields.

use ultrasql_core::{BlockNumber, DataType, Lsn, Oid, Schema};

/// Fold a SQL identifier the same way the catalog stores lookup keys.
#[must_use]
pub fn fold_identifier(name: &str) -> String {
    name.to_ascii_lowercase()
}

/// Return the canonical table lookup key for a schema-qualified relation.
#[must_use]
pub fn table_lookup_key(schema_name: &str, table_name: &str) -> String {
    fn encoded_key(schema: &str, relation: &str) -> String {
        format!("{}:{schema}{}:{relation}", schema.len(), relation.len())
    }

    let schema = fold_identifier(schema_name);
    let table = fold_identifier(table_name);
    if schema.contains('.') || table.contains('.') {
        return encoded_key(&schema, &table);
    }
    match schema.as_str() {
        "public" | "pg_catalog" | "information_schema" => table,
        _ => format!("{schema}.{table}"),
    }
}

/// Decode an encoded table lookup key into `(schema, relation)`.
///
/// Returns `None` for legacy unencoded keys such as public relations
/// without dots. The returned string slices borrow from `key`.
#[must_use]
pub fn decode_table_lookup_key(key: &str) -> Option<(&str, &str)> {
    let (schema_len_text, rest) = key.split_once(':')?;
    if schema_len_text.is_empty() || !schema_len_text.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    let schema_len = schema_len_text.parse::<usize>().ok()?;
    let (schema, after_schema) = split_at_byte(rest, schema_len)?;
    let (relation_len_text, relation) = after_schema.split_once(':')?;
    if relation_len_text.is_empty() || !relation_len_text.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    let relation_len = relation_len_text.parse::<usize>().ok()?;
    (relation.len() == relation_len).then_some((schema, relation))
}

fn split_at_byte(text: &str, index: usize) -> Option<(&str, &str)> {
    (index <= text.len() && text.is_char_boundary(index)).then(|| text.split_at(index))
}

/// Return the canonical type lookup key for a schema-qualified type.
#[must_use]
pub fn type_lookup_key(schema_name: &str, type_name: &str) -> String {
    let schema = fold_identifier(schema_name);
    let ty = fold_identifier(type_name);
    format!("{}:{schema}{}:{ty}", schema.len(), ty.len())
}

/// Return the canonical lookup key for a schema-qualified index.
#[must_use]
pub fn index_lookup_key(schema_name: &str, index_name: &str) -> String {
    table_lookup_key(schema_name, index_name)
}

/// A table (relation) entry in the catalog.
///
/// The owning catalog hands out cloned `TableEntry` values rather than
/// borrowed references. This keeps the API uniform between the in-memory
/// implementation (where a clone is cheap) and the future persistent
/// implementation (where the entry is materialized from a heap page and
/// the borrow would tie up a buffer pin).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableEntry {
    /// Catalog-wide object identifier. Stable for the life of the table.
    pub oid: Oid,
    /// Bare relation name (without schema qualifier).
    pub name: String,
    /// Schema (namespace) the table lives in. `"public"` by default.
    pub schema_name: String,
    /// Ordered column metadata.
    pub schema: Schema,
    /// LSN at which the CREATE TABLE record was committed. Useful for
    /// crash recovery and time-travel queries; ignored by the in-memory
    /// implementation today.
    pub created_at_lsn: Lsn,
    /// Estimated number of heap blocks. The optimizer uses this as a
    /// size hint for sequential-scan costing. Update via
    /// [`crate::MutableCatalog::update_table_size`] when ANALYZE or a
    /// bulk load completes.
    pub n_blocks: u32,
    /// First heap page of this table. `BlockNumber::INVALID` for tables
    /// that have not been materialized yet (CREATE TABLE without any
    /// inserts).
    pub root_block: BlockNumber,
    /// Relation storage options supplied by `ALTER TABLE ... SET (...)`.
    pub options: Vec<(String, String)>,
}

impl TableEntry {
    /// Construct a `TableEntry` with default size statistics.
    ///
    /// Defaults: `created_at_lsn = Lsn::ZERO`, `n_blocks = 0`,
    /// `root_block = BlockNumber::INVALID`. Callers that need exact
    /// values should build the struct literally.
    #[must_use]
    pub fn new<N: Into<String>>(oid: Oid, name: N, schema_name: N, schema: Schema) -> Self {
        Self {
            oid,
            name: name.into(),
            schema_name: schema_name.into(),
            schema,
            created_at_lsn: Lsn::ZERO,
            n_blocks: 0,
            root_block: BlockNumber::INVALID,
            options: Vec::new(),
        }
    }

    /// Attach relation storage options.
    #[must_use]
    pub fn with_options(mut self, options: Vec<(String, String)>) -> Self {
        self.options = options;
        self
    }
}

/// An index entry in the catalog.
///
/// Mirrors the fields of `pg_index` that the planner needs: which table
/// the index covers, which columns (by attnum) it indexes, where its
/// root page lives, which access method/opclasses were requested, and
/// whether duplicates are forbidden.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexEntry {
    /// Catalog-wide object identifier for the index itself.
    pub oid: Oid,
    /// Bare index name.
    pub name: String,
    /// Schema (namespace) the index lives in. Indexes live in the same
    /// namespace as their parent relation.
    pub schema_name: String,
    /// OID of the underlying table.
    pub table_oid: Oid,
    /// Column attnums (0-based positions into the table's schema) the
    /// index covers, in declaration order. Composite indexes carry
    /// multiple entries.
    pub columns: Vec<u16>,
    /// Root page of the index B+ tree.
    pub root_block: BlockNumber,
    /// Whether this index enforces uniqueness.
    pub is_unique: bool,
    /// Access method requested by `CREATE INDEX ... USING`.
    pub access_method: String,
    /// Opclass names supplied per key column.
    pub opclasses: Vec<Option<String>>,
    /// Storage options supplied in `WITH (...)`.
    pub options: Vec<(String, String)>,
}

impl IndexEntry {
    /// Construct an `IndexEntry` with `root_block = BlockNumber::INVALID`.
    ///
    /// The root block becomes meaningful once the index is materialized
    /// (the executor allocates the first leaf and rewrites the entry via
    /// a follow-up update path, parallel to PostgreSQL's
    /// `RelationSetNewRelfilenode`).
    #[must_use]
    pub fn new<N: Into<String>>(
        oid: Oid,
        name: N,
        table_oid: Oid,
        columns: Vec<u16>,
        is_unique: bool,
    ) -> Self {
        Self {
            oid,
            name: name.into(),
            schema_name: "public".to_owned(),
            table_oid,
            columns,
            root_block: BlockNumber::INVALID,
            is_unique,
            access_method: "btree".to_owned(),
            opclasses: Vec::new(),
            options: Vec::new(),
        }
    }

    /// Attach the index namespace.
    #[must_use]
    pub fn with_schema_name<S: Into<String>>(mut self, schema_name: S) -> Self {
        self.schema_name = schema_name.into();
        self
    }

    /// Attach an access method and per-column opclasses.
    #[must_use]
    pub fn with_access_method<M: Into<String>>(
        mut self,
        method: M,
        opclasses: Vec<Option<String>>,
    ) -> Self {
        self.access_method = method.into();
        self.opclasses = opclasses;
        self
    }

    /// Attach storage options captured from `WITH (...)`.
    #[must_use]
    pub fn with_options(mut self, options: Vec<(String, String)>) -> Self {
        self.options = options;
        self
    }
}

/// One label belonging to a user-defined enum type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnumLabelEntry {
    /// Catalog-wide object identifier for the `pg_enum` row.
    pub oid: Oid,
    /// Label text exactly as stored for comparisons and display.
    pub label: String,
    /// Declaration-order position. PostgreSQL exposes this as
    /// `pg_enum.enumsortorder`; UltraSQL stores it as an integer to keep the
    /// catalog entry deterministic and converts to `real` at the SQL view.
    pub sort_order: u32,
}

/// A user-defined enum type entry in the catalog.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnumTypeEntry {
    /// `pg_type.oid` for the enum type.
    pub oid: Oid,
    /// Bare type name, case-folded for unquoted identifiers.
    pub name: String,
    /// SQL namespace, usually `"public"`.
    pub schema_name: String,
    /// Ordered labels accepted by this type.
    pub labels: Vec<EnumLabelEntry>,
}

impl EnumTypeEntry {
    /// Return the planner/executor [`DataType`] carried by columns of this
    /// enum type.
    #[must_use]
    pub fn data_type(&self) -> DataType {
        DataType::Enum {
            oid: self.oid,
            name: self.name.clone().into(),
            labels: self
                .labels
                .iter()
                .map(|label| label.label.clone())
                .collect::<Vec<_>>()
                .into(),
        }
    }
}

/// A user-defined composite type entry in the catalog.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompositeTypeEntry {
    /// `pg_type.oid` and `pg_class.oid` for the composite row type.
    pub oid: Oid,
    /// Bare type name, case-folded for unquoted identifiers.
    pub name: String,
    /// SQL namespace, usually `"public"`.
    pub schema_name: String,
    /// Ordered attribute metadata. Composite attributes are nullable in
    /// PostgreSQL's `CREATE TYPE ... AS (...)` form.
    pub schema: Schema,
}

impl CompositeTypeEntry {
    /// Return the planner/executor [`DataType`] carried by columns of this
    /// composite type.
    #[must_use]
    pub fn data_type(&self) -> DataType {
        DataType::Composite {
            oid: self.oid,
            name: self.name.clone().into(),
            fields: self
                .schema
                .fields()
                .iter()
                .map(|field| (field.name.clone(), field.data_type.clone()))
                .collect::<Vec<_>>()
                .into(),
        }
    }
}

/// A user-defined domain type entry in the catalog.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DomainTypeEntry {
    /// `pg_type.oid` for the domain type.
    pub oid: Oid,
    /// Bare type name, case-folded for unquoted identifiers.
    pub name: String,
    /// SQL namespace, usually `"public"`.
    pub schema_name: String,
    /// Underlying base type used for storage.
    pub base_type: DataType,
    /// Domain-level NOT NULL constraint.
    pub not_null: bool,
}

impl DomainTypeEntry {
    /// Return the planner/executor [`DataType`] carried by columns of this
    /// domain type.
    #[must_use]
    pub fn data_type(&self) -> DataType {
        DataType::Domain {
            oid: self.oid,
            name: self.name.clone().into(),
            base_type: Box::new(self.base_type.clone()),
            not_null: self.not_null,
        }
    }
}

#[cfg(test)]
mod tests {
    use ultrasql_core::{DataType, Field};

    use super::*;

    fn sample_schema() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int64),
            Field::nullable("name", DataType::Text { max_len: None }),
        ])
        .expect("schema invariants hold for test fixture")
    }

    #[test]
    fn table_entry_defaults_are_safe() {
        let entry = TableEntry::new(Oid::new(16384), "users", "public", sample_schema());
        assert_eq!(entry.n_blocks, 0);
        assert_eq!(entry.root_block, BlockNumber::INVALID);
        assert_eq!(entry.created_at_lsn, Lsn::ZERO);
        assert_eq!(entry.name, "users");
        assert_eq!(entry.schema_name, "public");
    }

    #[test]
    fn index_entry_defaults_are_safe() {
        let entry = IndexEntry::new(Oid::new(16385), "users_pk", Oid::new(16384), vec![0], true);
        assert_eq!(entry.root_block, BlockNumber::INVALID);
        assert!(entry.is_unique);
        assert_eq!(entry.columns, vec![0]);
        assert_eq!(entry.table_oid, Oid::new(16384));
    }

    #[test]
    fn type_lookup_key_distinguishes_dots_in_schema_and_type_names() {
        assert_ne!(
            type_lookup_key("app", "mood.type"),
            type_lookup_key("app.mood", "type")
        );
        assert_eq!(
            type_lookup_key("App", "Mood.Type"),
            type_lookup_key("app", "mood.type")
        );
    }

    #[test]
    fn table_lookup_key_distinguishes_dots_in_schema_and_relation_names() {
        assert_eq!(table_lookup_key("public", "orders"), "orders");
        assert_eq!(table_lookup_key("app", "orders"), "app.orders");
        assert_ne!(
            table_lookup_key("app", "events.log"),
            table_lookup_key("app.events", "log")
        );
        assert_eq!(
            table_lookup_key("App", "Events.Log"),
            table_lookup_key("app", "events.log")
        );
    }

    #[test]
    fn decode_table_lookup_key_round_trips_encoded_relation_keys() {
        let public_dotted = table_lookup_key("public", "events.log");
        assert_eq!(
            decode_table_lookup_key(&public_dotted),
            Some(("public", "events.log"))
        );

        let schema_dotted = table_lookup_key("app.events", "log");
        assert_eq!(
            decode_table_lookup_key(&schema_dotted),
            Some(("app.events", "log"))
        );

        assert_eq!(decode_table_lookup_key("orders"), None);
        assert_eq!(decode_table_lookup_key("3:app4:logsx"), None);
    }
}
