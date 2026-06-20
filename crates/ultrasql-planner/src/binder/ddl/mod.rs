//! DDL binders. Split out of `binder/mod.rs` to keep each
//! production source file under the 600-line ceiling.
//!
//! Public entry points are `pub(super)` so the dispatch in
//! `binder::bind` can route to them; internal helpers
//! (`object_name_namespace`, `resolve_type_name`,
//! `resolve_column_nullability`, `synthesise_index_name`) stay
//! private to this module.
//!
//! The binders are grouped by statement family across submodules:
//! [`create_table`] for `CREATE TABLE`, [`types`] for the type-defining
//! statements and type-name resolution, [`view`] for the view family
//! and `CREATE POLICY`, [`sequence`] for sequences/schemas/roles/comments,
//! [`index`] for index and `DROP` statements, [`alter_table`] for
//! `ALTER TABLE`/`TRUNCATE`, and [`copy`] for `COPY`. Cross-cutting
//! helpers live in [`shared`].

mod alter_table;
mod copy;
mod create_table;
mod index;
mod sequence;
mod shared;
mod types;
mod view;

pub(super) use alter_table::{bind_alter_table, bind_truncate};
pub(super) use copy::bind_copy;
pub(super) use create_table::bind_create_table;
pub(super) use index::{bind_create_index, bind_drop_index, bind_drop_table};
pub(super) use sequence::{
    bind_alter_role, bind_alter_sequence, bind_comment, bind_create_role, bind_create_schema,
    bind_create_sequence, bind_drop_role, bind_drop_schema, bind_drop_sequence,
};
pub(super) use types::{
    bind_create_domain, bind_create_operator, bind_create_type, resolve_type_name,
};
pub(super) use view::{
    bind_alter_view, bind_create_materialized_view, bind_create_policy, bind_create_view,
};
