//! Planner-facing catalog abstraction.
//!
//! The binder needs a way to resolve a table name to its [`Schema`]. To
//! keep `ultrasql-planner` decoupled from the real catalog (which lives
//! behind MVCC machinery), the planner consumes a small [`Catalog`]
//! trait whose only requirement is table lookup. [`InMemoryCatalog`] is
//! a hash-map-backed implementation used by tests and by short-lived
//! tools (the REPL, EXPLAIN-only tooling) that do not need the full
//! catalog stack.
//!
//! The longer-term plan is to migrate this trait into
//! `ultrasql-catalog` via an RFC; defining it locally here keeps the
//! current bring-up from blocking on that decision.

use std::collections::{HashMap, HashSet};

use ultrasql_core::{DataType, Oid, Schema};

const PG_OID_BOOL: u32 = 16;
const PG_OID_BYTEA: u32 = 17;
const PG_OID_INT8: u32 = 20;
const PG_OID_INT2: u32 = 21;
const PG_OID_INT4: u32 = 23;
const PG_OID_TEXT: u32 = 25;
const PG_OID_OID: u32 = 26;
const PG_OID_CIDR: u32 = 650;
const PG_OID_FLOAT4: u32 = 700;
const PG_OID_FLOAT8: u32 = 701;
const PG_OID_MACADDR8: u32 = 774;
const PG_OID_MONEY: u32 = 790;
const PG_OID_MACADDR: u32 = 829;
const PG_OID_INET: u32 = 869;
const PG_OID_BPCHAR: u32 = 1042;
const PG_OID_VARCHAR: u32 = 1043;
const PG_OID_DATE: u32 = 1082;
const PG_OID_TIME: u32 = 1083;
const PG_OID_TIMESTAMP: u32 = 1114;
const PG_OID_TIMESTAMPTZ: u32 = 1184;
const PG_OID_TIMETZ: u32 = 1266;
const PG_OID_NUMERIC: u32 = 1700;
const PG_OID_REGCLASS: u32 = 2205;
const PG_OID_REGTYPE: u32 = 2206;
const PG_OID_UUID: u32 = 2950;
const PG_OID_PG_LSN: u32 = 3220;
const PG_OID_JSON: u32 = 114;
const PG_OID_JSONB: u32 = 3802;
const PG_OID_XML: u32 = 142;

/// Metadata about a single table, sufficient for binding.
///
/// Indexes, statistics, and constraints are *not* present at this
/// layer; the binder only needs to validate column references and
/// shape the produced [`crate::plan::LogicalPlan::Scan`] node. The
/// optimizer can fetch the richer view through a different trait.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableMeta {
    /// Case-folded SQL namespace that owns this table.
    pub schema_name: String,
    /// Ordered list of columns and their types.
    pub schema: Schema,
}

impl TableMeta {
    /// Construct a `TableMeta` over a schema in the `public` namespace.
    #[must_use]
    pub fn new(schema: Schema) -> Self {
        Self::with_schema_name("public", schema)
    }

    /// Construct a `TableMeta` over a schema in a specific namespace.
    #[must_use]
    pub fn with_schema_name(schema_name: impl Into<String>, schema: Schema) -> Self {
        Self {
            schema_name: schema_name.into().to_ascii_lowercase(),
            schema,
        }
    }
}

/// Catalog trait consumed by the binder.
///
/// Implementations must be cheap to call: the binder may issue several
/// lookups for a single statement. Implementations are required to be
/// `Send + Sync` so a single catalog handle can be shared across the
/// planner's worker threads.
pub trait Catalog: Send + Sync {
    /// Resolve a (case-insensitive) table name.
    ///
    /// Returns `None` if no table by that name is registered.
    fn lookup_table(&self, name: &str) -> Option<TableMeta>;

    /// Resolve a table by schema and bare relation name.
    fn lookup_table_in_schema(&self, schema_name: &str, name: &str) -> Option<TableMeta> {
        self.lookup_table(name)
            .filter(|meta| meta.schema_name.eq_ignore_ascii_case(schema_name))
    }

    /// Resolve a user-defined type by its case-folded SQL name.
    ///
    /// Built-in types are resolved directly by the binder; catalog
    /// implementations only need to return user-defined type metadata such as
    /// enum OIDs and labels. The default keeps existing lightweight catalogs
    /// valid when they do not model custom types.
    fn lookup_type(&self, name: &str) -> Option<DataType> {
        let _ = name;
        None
    }

    /// Resolve a user-defined type by schema and bare type name.
    fn lookup_type_in_schema(&self, schema_name: &str, name: &str) -> Option<DataType> {
        if schema_name.eq_ignore_ascii_case("public") {
            self.lookup_type(name)
        } else {
            self.lookup_type(&ultrasql_catalog::type_lookup_key(schema_name, name))
        }
    }

    /// Return whether a case-insensitive index name exists.
    fn lookup_index(&self, name: &str) -> bool {
        let _ = name;
        false
    }

    /// Return whether an index exists in a specific schema.
    fn lookup_index_in_schema(&self, schema_name: &str, name: &str) -> bool {
        self.lookup_index(&ultrasql_catalog::index_lookup_key(schema_name, name))
    }

    /// Resolve the schema that owns an unqualified index name.
    fn lookup_index_schema(&self, name: &str) -> Option<String> {
        if self.lookup_index_in_schema("public", name) {
            return Some("public".to_owned());
        }
        if let Some((schema_name, index_name)) = name.rsplit_once('.') {
            return self
                .lookup_index_in_schema(schema_name, index_name)
                .then(|| schema_name.to_ascii_lowercase());
        }
        None
    }

    /// Resolve a relation name to its catalog OID.
    fn lookup_table_oid(&self, name: &str) -> Option<Oid> {
        let _ = name;
        None
    }

    /// Resolve a relation name to its catalog OID inside one schema.
    fn lookup_table_oid_in_schema(&self, schema_name: &str, name: &str) -> Option<Oid> {
        if schema_name.eq_ignore_ascii_case("public") {
            self.lookup_table_oid(name).or_else(|| {
                self.lookup_table_oid(&ultrasql_catalog::table_lookup_key("public", name))
            })
        } else {
            self.lookup_table_oid(&ultrasql_catalog::table_lookup_key(schema_name, name))
        }
    }

    /// Return whether an unqualified table reference can see this schema.
    fn table_schema_visible_without_qualification(&self, schema_name: &str) -> bool {
        matches!(
            schema_name.to_ascii_lowercase().as_str(),
            "public" | "pg_catalog" | "information_schema"
        )
    }

    /// Resolve a type name to its `pg_type.oid`.
    fn lookup_type_oid(&self, name: &str) -> Option<Oid> {
        builtin_type_oid(name)
    }

    /// Resolve a type name in a specific schema to its `pg_type.oid`.
    fn lookup_type_oid_in_schema(&self, schema_name: &str, name: &str) -> Option<Oid> {
        if schema_name.eq_ignore_ascii_case("pg_catalog") {
            builtin_type_oid(name).or_else(|| self.lookup_type_oid(name))
        } else if schema_name.eq_ignore_ascii_case("public") {
            self.lookup_type_oid(name)
        } else {
            self.lookup_type_oid(&ultrasql_catalog::type_lookup_key(schema_name, name))
        }
    }
}

/// Simple hash-map catalog used by tests and by callers that do not
/// need MVCC-aware lookup.
///
/// Lookups are case-insensitive: the stored key is the ASCII
/// lowercase of the inserted name. Callers that need to register a
/// case-sensitive (quoted) identifier should fold their key
/// themselves before insertion.
#[derive(Clone, Debug, Default)]
pub struct InMemoryCatalog {
    tables: HashMap<String, TableMeta>,
    types: HashMap<String, DataType>,
    indexes: HashSet<String>,
}

impl InMemoryCatalog {
    /// Construct an empty catalog.
    #[must_use]
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            types: HashMap::new(),
            indexes: HashSet::new(),
        }
    }

    /// Register a table. If a table with the same case-folded name
    /// already exists, the previous entry is returned.
    pub fn register(&mut self, name: &str, meta: TableMeta) -> Option<TableMeta> {
        let key = if name.contains('.') {
            name.to_ascii_lowercase()
        } else {
            ultrasql_catalog::table_lookup_key(&meta.schema_name, name)
        };
        self.tables.insert(key, meta)
    }

    /// Register a user-defined type. If a type with the same case-folded name
    /// already exists, the previous entry is returned.
    pub fn register_type(&mut self, name: &str, data_type: DataType) -> Option<DataType> {
        self.types.insert(name.to_ascii_lowercase(), data_type)
    }

    /// Register an index name for DDL binding tests and lightweight tools.
    pub fn register_index(&mut self, name: &str) -> bool {
        self.indexes.insert(name.to_ascii_lowercase())
    }

    /// Register an index in a specific schema.
    pub fn register_index_in_schema(&mut self, schema_name: &str, name: &str) -> bool {
        self.indexes
            .insert(ultrasql_catalog::index_lookup_key(schema_name, name))
    }
}

impl Catalog for InMemoryCatalog {
    fn lookup_table(&self, name: &str) -> Option<TableMeta> {
        let folded = name.to_ascii_lowercase();
        self.tables.get(&folded).cloned().or_else(|| {
            let public_key = ultrasql_catalog::table_lookup_key("public", name);
            (public_key != folded)
                .then(|| self.tables.get(&public_key).cloned())
                .flatten()
        })
    }

    fn lookup_table_in_schema(&self, schema_name: &str, name: &str) -> Option<TableMeta> {
        self.tables
            .get(&ultrasql_catalog::table_lookup_key(schema_name, name))
            .cloned()
    }

    fn lookup_type(&self, name: &str) -> Option<DataType> {
        self.types.get(&name.to_ascii_lowercase()).cloned()
    }

    fn lookup_type_in_schema(&self, schema_name: &str, name: &str) -> Option<DataType> {
        self.types
            .get(&ultrasql_catalog::type_lookup_key(schema_name, name))
            .cloned()
            .or_else(|| {
                schema_name
                    .eq_ignore_ascii_case("public")
                    .then(|| self.lookup_type(name))
                    .flatten()
            })
    }

    fn lookup_index(&self, name: &str) -> bool {
        let folded = name.to_ascii_lowercase();
        self.indexes.contains(&folded) || {
            let public_key = ultrasql_catalog::index_lookup_key("public", name);
            public_key != folded && self.indexes.contains(&public_key)
        }
    }

    fn lookup_index_in_schema(&self, schema_name: &str, name: &str) -> bool {
        self.indexes
            .contains(&ultrasql_catalog::index_lookup_key(schema_name, name))
    }

    fn lookup_type_oid(&self, name: &str) -> Option<Oid> {
        self.lookup_type(name)
            .as_ref()
            .and_then(type_oid_for_data_type)
            .or_else(|| builtin_type_oid(name))
    }

    fn lookup_type_oid_in_schema(&self, schema_name: &str, name: &str) -> Option<Oid> {
        self.lookup_type_in_schema(schema_name, name)
            .as_ref()
            .and_then(type_oid_for_data_type)
            .or_else(|| {
                schema_name
                    .eq_ignore_ascii_case("pg_catalog")
                    .then(|| builtin_type_oid(name))
                    .flatten()
            })
    }
}

/// Wire OID for a built-in type name.
#[must_use]
pub fn builtin_type_oid(name: &str) -> Option<Oid> {
    let folded = name.to_ascii_lowercase();
    let raw = match folded.as_str() {
        "bool" | "boolean" => PG_OID_BOOL,
        "bytea" => PG_OID_BYTEA,
        "bigint" | "int8" => PG_OID_INT8,
        "smallint" | "int2" => PG_OID_INT2,
        "int" | "integer" | "int4" => PG_OID_INT4,
        "text" => PG_OID_TEXT,
        "oid" => PG_OID_OID,
        "cidr" => PG_OID_CIDR,
        "real" | "float4" => PG_OID_FLOAT4,
        "double" | "double precision" | "float" | "float8" => PG_OID_FLOAT8,
        "macaddr8" => PG_OID_MACADDR8,
        "money" => PG_OID_MONEY,
        "macaddr" => PG_OID_MACADDR,
        "inet" => PG_OID_INET,
        "char" | "character" | "bpchar" => PG_OID_BPCHAR,
        "varchar" | "character varying" => PG_OID_VARCHAR,
        "date" => PG_OID_DATE,
        "time" | "time without time zone" => PG_OID_TIME,
        "timestamp" | "timestamp without time zone" => PG_OID_TIMESTAMP,
        "timestamptz" | "timestamp with time zone" => PG_OID_TIMESTAMPTZ,
        "timetz" | "time with time zone" => PG_OID_TIMETZ,
        "numeric" | "decimal" => PG_OID_NUMERIC,
        "regclass" => PG_OID_REGCLASS,
        "regtype" => PG_OID_REGTYPE,
        "uuid" => PG_OID_UUID,
        "pg_lsn" => PG_OID_PG_LSN,
        "json" => PG_OID_JSON,
        "jsonb" => PG_OID_JSONB,
        "xml" => PG_OID_XML,
        _ => return None,
    };
    Some(Oid::new(raw))
}

fn type_oid_for_data_type(data_type: &DataType) -> Option<Oid> {
    match data_type {
        DataType::Enum { oid, .. }
        | DataType::Composite { oid, .. }
        | DataType::Domain { oid, .. } => Some(*oid),
        other => builtin_type_oid(&other.to_string()),
    }
}

/// Adapter so the binder can read from a persistent
/// [`ultrasql_catalog::CatalogSnapshot`] directly.
///
/// The persistent catalog hands out immutable snapshots for wait-free
/// reads; this impl projects each `TableEntry` down to the
/// schema-only [`TableMeta`] the binder needs. `lookup_index` and other
/// catalog APIs do not flow through the planner trait, so they are not
/// exposed here.
///
/// The case-folding contract is the same as [`InMemoryCatalog`]: the
/// snapshot stores names already folded to ASCII lowercase, so we fold
/// the query before lookup.
impl Catalog for ultrasql_catalog::CatalogSnapshot {
    fn lookup_table(&self, name: &str) -> Option<TableMeta> {
        let folded = name.to_ascii_lowercase();
        self.tables
            .get(&folded)
            .map(|entry| TableMeta::with_schema_name(&entry.schema_name, entry.schema.clone()))
            .or_else(|| {
                let public_key = ultrasql_catalog::table_lookup_key("public", name);
                (public_key != folded)
                    .then(|| {
                        self.tables.get(&public_key).map(|entry| {
                            TableMeta::with_schema_name(&entry.schema_name, entry.schema.clone())
                        })
                    })
                    .flatten()
            })
    }

    fn lookup_table_in_schema(&self, schema_name: &str, name: &str) -> Option<TableMeta> {
        self.tables
            .get(&ultrasql_catalog::table_lookup_key(schema_name, name))
            .map(|entry| TableMeta::with_schema_name(&entry.schema_name, entry.schema.clone()))
    }

    fn lookup_type(&self, name: &str) -> Option<DataType> {
        let key = ultrasql_catalog::type_lookup_key("public", name);
        self.enum_types
            .get(&key)
            .map(ultrasql_catalog::EnumTypeEntry::data_type)
            .or_else(|| {
                self.composite_types
                    .get(&key)
                    .map(ultrasql_catalog::CompositeTypeEntry::data_type)
            })
            .or_else(|| {
                self.domain_types
                    .get(&key)
                    .map(ultrasql_catalog::DomainTypeEntry::data_type)
            })
            .or_else(|| {
                let key = name.to_ascii_lowercase();
                self.enum_types
                    .get(&key)
                    .map(ultrasql_catalog::EnumTypeEntry::data_type)
                    .or_else(|| {
                        self.composite_types
                            .get(&key)
                            .map(ultrasql_catalog::CompositeTypeEntry::data_type)
                    })
                    .or_else(|| {
                        self.domain_types
                            .get(&key)
                            .map(ultrasql_catalog::DomainTypeEntry::data_type)
                    })
            })
    }

    fn lookup_type_in_schema(&self, schema_name: &str, name: &str) -> Option<DataType> {
        let key = ultrasql_catalog::type_lookup_key(schema_name, name);
        self.enum_types
            .get(&key)
            .map(ultrasql_catalog::EnumTypeEntry::data_type)
            .or_else(|| {
                self.composite_types
                    .get(&key)
                    .map(ultrasql_catalog::CompositeTypeEntry::data_type)
            })
            .or_else(|| {
                self.domain_types
                    .get(&key)
                    .map(ultrasql_catalog::DomainTypeEntry::data_type)
            })
            .or_else(|| {
                schema_name
                    .eq_ignore_ascii_case("public")
                    .then(|| self.lookup_type(name))
                    .flatten()
            })
    }

    fn lookup_index(&self, name: &str) -> bool {
        let folded = name.to_ascii_lowercase();
        self.indexes.contains_key(&folded) || {
            let public_key = ultrasql_catalog::index_lookup_key("public", name);
            public_key != folded && self.indexes.contains_key(&public_key)
        }
    }

    fn lookup_index_in_schema(&self, schema_name: &str, name: &str) -> bool {
        self.indexes
            .contains_key(&ultrasql_catalog::index_lookup_key(schema_name, name))
    }

    fn lookup_index_schema(&self, name: &str) -> Option<String> {
        if let Some(entry) = self
            .indexes
            .get(&ultrasql_catalog::index_lookup_key("public", name))
        {
            return Some(entry.schema_name.clone());
        }
        if let Some((schema_name, index_name)) = name.rsplit_once('.') {
            return self
                .lookup_index_in_schema(schema_name, index_name)
                .then(|| schema_name.to_ascii_lowercase());
        }
        None
    }

    fn lookup_table_oid(&self, name: &str) -> Option<Oid> {
        let folded = name.to_ascii_lowercase();
        self.tables.get(&folded).map(|entry| entry.oid).or_else(|| {
            let public_key = ultrasql_catalog::table_lookup_key("public", name);
            (public_key != folded)
                .then(|| self.tables.get(&public_key).map(|entry| entry.oid))
                .flatten()
        })
    }

    fn lookup_type_oid(&self, name: &str) -> Option<Oid> {
        builtin_type_oid(name).or_else(|| {
            let key = ultrasql_catalog::type_lookup_key("public", name);
            self.enum_types
                .get(&key)
                .map(|entry| entry.oid)
                .or_else(|| self.composite_types.get(&key).map(|entry| entry.oid))
                .or_else(|| self.domain_types.get(&key).map(|entry| entry.oid))
                .or_else(|| {
                    let key = name.to_ascii_lowercase();
                    self.enum_types
                        .get(&key)
                        .map(|entry| entry.oid)
                        .or_else(|| self.composite_types.get(&key).map(|entry| entry.oid))
                        .or_else(|| self.domain_types.get(&key).map(|entry| entry.oid))
                })
        })
    }

    fn lookup_type_oid_in_schema(&self, schema_name: &str, name: &str) -> Option<Oid> {
        if schema_name.eq_ignore_ascii_case("pg_catalog") {
            return builtin_type_oid(name).or_else(|| self.lookup_type_oid(name));
        }
        let key = ultrasql_catalog::type_lookup_key(schema_name, name);
        self.enum_types
            .get(&key)
            .map(|entry| entry.oid)
            .or_else(|| self.composite_types.get(&key).map(|entry| entry.oid))
            .or_else(|| self.domain_types.get(&key).map(|entry| entry.oid))
            .or_else(|| {
                schema_name
                    .eq_ignore_ascii_case("public")
                    .then(|| self.lookup_type_oid(name))
                    .flatten()
            })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ultrasql_core::{DataType, Field, Oid};

    use super::*;

    fn users_schema() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int32),
            Field::nullable("name", DataType::Text { max_len: None }),
            Field::nullable("score", DataType::Float64),
        ])
        .expect("schema invariants hold for test fixture")
    }

    #[test]
    fn lookup_round_trips_case_insensitively() {
        let mut cat = InMemoryCatalog::new();
        cat.register("Users", TableMeta::new(users_schema()));
        assert!(cat.lookup_table("users").is_some());
        assert!(cat.lookup_table("USERS").is_some());
        assert!(cat.lookup_table("UsErS").is_some());
        assert!(cat.lookup_table("orders").is_none());
    }

    #[test]
    fn register_returns_previous_entry() {
        let mut cat = InMemoryCatalog::new();
        let first = TableMeta::new(users_schema());
        assert!(cat.register("users", first.clone()).is_none());
        let replacement = TableMeta::new(
            Schema::new([Field::required("only", DataType::Int64)])
                .expect("schema invariants hold for test fixture"),
        );
        let previous = cat.register("users", replacement);
        assert_eq!(previous, Some(first));
    }

    #[test]
    fn builtin_type_oids_cover_supported_aliases() {
        for (name, oid) in [
            ("bool", 16),
            ("boolean", 16),
            ("bytea", 17),
            ("bigint", 20),
            ("int8", 20),
            ("smallint", 21),
            ("int2", 21),
            ("int", 23),
            ("integer", 23),
            ("int4", 23),
            ("text", 25),
            ("oid", 26),
            ("cidr", 650),
            ("real", 700),
            ("float4", 700),
            ("double precision", 701),
            ("float8", 701),
            ("macaddr8", 774),
            ("money", 790),
            ("macaddr", 829),
            ("inet", 869),
            ("char", 1042),
            ("bpchar", 1042),
            ("varchar", 1043),
            ("date", 1082),
            ("time without time zone", 1083),
            ("timestamp with time zone", 1184),
            ("timetz", 1266),
            ("numeric", 1700),
            ("decimal", 1700),
            ("regclass", 2205),
            ("regtype", 2206),
            ("uuid", 2950),
            ("pg_lsn", 3220),
            ("json", 114),
            ("jsonb", 3802),
            ("xml", 142),
        ] {
            assert_eq!(builtin_type_oid(name), Some(Oid::new(oid)), "{name}");
        }
        assert_eq!(builtin_type_oid("does_not_exist"), None);
    }

    #[test]
    fn registered_user_type_oids_override_builtin_fallback() {
        let mut cat = InMemoryCatalog::new();
        let enum_type = DataType::Enum {
            oid: Oid::new(42_001),
            name: Arc::from("mood"),
            labels: Arc::from([String::from("sad"), String::from("ok")]),
        };
        assert!(cat.register_type("Mood", enum_type.clone()).is_none());
        assert_eq!(cat.lookup_type("mood"), Some(enum_type));
        assert_eq!(cat.lookup_type_oid("MOOD"), Some(Oid::new(42_001)));
        assert_eq!(cat.lookup_type_oid("int4"), Some(Oid::new(23)));
    }
}
