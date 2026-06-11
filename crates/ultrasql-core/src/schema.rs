//! Schema descriptors.
//!
//! A [`Schema`] is the ordered list of [`Field`]s that describes a row.
//! It is the type-system anchor used by every operator that produces or
//! consumes rows.
//!
//! Schemas are immutable once built — operations that "modify" a schema
//! (projection pushdown, column rename, etc.) construct a new `Schema`.

use std::fmt;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::types::DataType;

/// A single column of a relation or expression result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    /// Column name. For computed columns, this is the SQL output alias.
    pub name: String,
    /// Column data type.
    pub data_type: DataType,
    /// Whether the column may contain SQL NULLs.
    pub nullable: bool,
}

impl Field {
    /// Construct a nullable field.
    pub fn nullable<N: Into<String>>(name: N, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: true,
        }
    }

    /// Construct a non-nullable field.
    pub fn required<N: Into<String>>(name: N, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: false,
        }
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)?;
        if !self.nullable {
            f.write_str(" NOT NULL")?;
        }
        Ok(())
    }
}

/// Ordered, named collection of [`Field`]s.
///
/// Internally stored as an `Arc<[Field]>` so cheap clones travel
/// through plans, batches, and operators.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    fields: Arc<[Field]>,
}

impl Schema {
    /// Build a schema from a sequence of fields.
    ///
    /// Returns an error if two fields share a name (after case-folding;
    /// SQL identifiers compare case-insensitively unless quoted).
    pub fn new<I: IntoIterator<Item = Field>>(fields: I) -> Result<Self> {
        let fields: Vec<Field> = fields.into_iter().collect();
        let mut seen: ahash::AHashSet<String> = ahash::AHashSet::with_capacity(fields.len());
        for f in &fields {
            let folded = f.name.to_lowercase();
            if !seen.insert(folded.clone()) {
                return Err(Error::invalid(format!(
                    "duplicate column name '{}' in schema",
                    f.name
                )));
            }
        }
        Ok(Self {
            fields: fields.into(),
        })
    }

    /// Build a schema that preserves duplicate SQL output labels.
    ///
    /// PostgreSQL permits result sets such as `SELECT f(), f()` to expose the
    /// same column label twice. This constructor is intended for final
    /// projection/RETURNING schemas where fields are addressed by ordinal on
    /// the wire. Name lookup methods continue to return the first matching
    /// field, so internal relation schemas should keep using [`Schema::new`].
    #[must_use]
    pub fn new_with_duplicate_names<I: IntoIterator<Item = Field>>(fields: I) -> Self {
        let fields: Vec<Field> = fields.into_iter().collect();
        Self {
            fields: fields.into(),
        }
    }

    /// Build an empty schema (zero columns). Used for plans that
    /// produce no projected columns (e.g., `EXISTS` subqueries
    /// pre-rewrite).
    #[must_use]
    pub fn empty() -> Self {
        Self {
            fields: Arc::from(Vec::<Field>::new()),
        }
    }

    /// Borrow the field slice.
    #[must_use]
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    /// Number of columns.
    #[must_use]
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Whether this schema has zero columns.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Look up a field by name, with case-folding. Returns
    /// `(index, &Field)` on hit.
    #[must_use]
    pub fn find(&self, name: &str) -> Option<(usize, &Field)> {
        self.fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name.eq_ignore_ascii_case(name))
    }

    /// Look up a field by name, returning an error on miss.
    pub fn index_of(&self, name: &str) -> Result<usize> {
        self.find(name)
            .map(|(i, _)| i)
            .ok_or_else(|| Error::not_found(format!("column '{name}'")))
    }

    /// Field by position. Panics if out of range; the variant returning
    /// `Option` is [`Schema::field`].
    #[must_use]
    pub fn field_at(&self, i: usize) -> &Field {
        &self.fields[i]
    }

    /// Field by position, `None` if out of range.
    #[must_use]
    pub fn field(&self, i: usize) -> Option<&Field> {
        self.fields.get(i)
    }

    /// Project this schema down to a subset of columns specified by
    /// 0-based indices, preserving the order in `indices`.
    pub fn project(&self, indices: &[usize]) -> Result<Self> {
        let mut out = Vec::with_capacity(indices.len());
        for &i in indices {
            if i >= self.fields.len() {
                return Err(Error::invalid(format!(
                    "projection index {i} out of bounds for schema of width {}",
                    self.fields.len()
                )));
            }
            out.push(self.fields[i].clone());
        }
        Self::new(out)
    }

    /// Concatenate two schemas (used for join output). Errors on name
    /// collision; callers must alias before concatenating.
    pub fn concat(&self, other: &Self) -> Result<Self> {
        let mut combined = Vec::with_capacity(combined_field_capacity(self.len(), other.len())?);
        combined.extend(self.fields.iter().cloned());
        combined.extend(other.fields.iter().cloned());
        Self::new(combined)
    }
}

fn combined_field_capacity(left: usize, right: usize) -> Result<usize> {
    left.checked_add(right)
        .ok_or_else(|| Error::invalid("schema width overflow"))
}

impl fmt::Display for Schema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("(")?;
        for (i, fld) in self.fields.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{fld}")?;
        }
        f.write_str(")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s() -> Schema {
        Schema::new([
            Field::required("id", DataType::Int64),
            Field::nullable("name", DataType::Text { max_len: None }),
            Field::nullable("score", DataType::Float64),
        ])
        .unwrap()
    }

    #[test]
    fn empty_schema_is_empty() {
        let e = Schema::empty();
        assert_eq!(e.len(), 0);
        assert!(e.is_empty());
    }

    #[test]
    fn lookup_is_case_insensitive() {
        let sc = s();
        assert_eq!(sc.find("ID").map(|(i, _)| i), Some(0));
        assert_eq!(sc.find("Name").map(|(i, _)| i), Some(1));
        assert!(sc.find("nope").is_none());
        assert_eq!(sc.index_of("score").unwrap(), 2);
        assert!(sc.index_of("nope").is_err());
    }

    #[test]
    fn duplicate_columns_rejected() {
        let dup = Schema::new([
            Field::required("a", DataType::Int32),
            Field::required("A", DataType::Int32),
        ]);
        assert!(dup.is_err());
    }

    #[test]
    fn output_schema_preserves_duplicate_labels() {
        let schema = Schema::new_with_duplicate_names([
            Field::required("pg_get_expr", DataType::Text { max_len: None }),
            Field::required("pg_get_expr", DataType::Text { max_len: None }),
        ]);

        assert_eq!(schema.len(), 2);
        assert_eq!(schema.field_at(0).name, "pg_get_expr");
        assert_eq!(schema.field_at(1).name, "pg_get_expr");
        assert_eq!(schema.index_of("pg_get_expr").unwrap(), 0);
    }

    #[test]
    fn projection_reorders_and_filters() {
        let sc = s();
        let proj = sc.project(&[2, 0]).unwrap();
        assert_eq!(proj.len(), 2);
        assert_eq!(proj.field_at(0).name, "score");
        assert_eq!(proj.field_at(1).name, "id");
    }

    #[test]
    fn projection_rejects_out_of_bounds() {
        let sc = s();
        assert!(sc.project(&[0, 99]).is_err());
    }

    #[test]
    fn concat_rejects_name_collisions() {
        let a = Schema::new([Field::required("x", DataType::Int32)]).unwrap();
        let b = Schema::new([Field::required("x", DataType::Int32)]).unwrap();
        assert!(a.concat(&b).is_err());
    }

    #[test]
    fn concat_succeeds_on_disjoint_names() {
        let a = Schema::new([Field::required("x", DataType::Int32)]).unwrap();
        let b = Schema::new([Field::required("y", DataType::Int32)]).unwrap();
        let c = a.concat(&b).unwrap();
        assert_eq!(c.len(), 2);
    }

    #[test]
    fn concat_capacity_rejects_overflow() {
        assert!(combined_field_capacity(usize::MAX, 1).is_err());
    }

    #[test]
    fn display_round_trip() {
        let sc = s();
        let s = sc.to_string();
        assert!(s.contains("id"));
        assert!(s.contains("bigint"));
        assert!(s.contains("NOT NULL"));
    }
}
