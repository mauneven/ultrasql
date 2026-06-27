//! Logical database dump and restore support for `EXPORT DATABASE` / `IMPORT DATABASE`.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt::Write as _;
use std::fs::File;
use std::io::{self, Read, Write as _};
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncWrite};
use ultrasql_catalog::{CatalogSnapshot, IndexEntry, TableEntry};
use ultrasql_core::{DataType, RelationId, Value};
use ultrasql_parser::Parser;
use ultrasql_parser::ast::Statement;
use ultrasql_planner::LogicalPlan;
use ultrasql_storage::sequence::SequenceSnapshot;
use ultrasql_txn::{IsolationLevel, Transaction};

use super::Session;
use crate::error::ServerError;
use crate::result_encoder::{SelectResult, run_ddl_command};
use crate::{TxnState, builtin_schema_name};

const DUMP_FORMAT: &str = "ultrasql.logical_dump";
const DUMP_VERSION: u32 = 1;
const MANIFEST_FILE: &str = "manifest.json";
const CHECKSUMS_FILE: &str = "checksums.json";
const SCHEMA_FILE: &str = "schema.sql";
const DATA_DIR: &str = "data";

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DumpManifest {
    format: String,
    version: u32,
    product_version: String,
    schema_file: String,
    schemas: Vec<DumpSchema>,
    sequences: Vec<DumpSequence>,
    tables: Vec<DumpTable>,
    indexes: Vec<DumpIndex>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DumpSchema {
    name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DumpSequence {
    schema: String,
    name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DumpTable {
    schema: String,
    name: String,
    data_file: String,
    row_count: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DumpIndex {
    schema: String,
    name: String,
    table_schema: String,
    table_name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DumpChecksums {
    algorithm: String,
    files: Vec<DumpFileChecksum>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DumpFileChecksum {
    path: String,
    sha256: String,
    bytes: u64,
}

#[derive(Debug)]
struct ExportedDataFile {
    manifest_entry: DumpTable,
    bytes: Vec<u8>,
}

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn execute_export_database(
        &mut self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::ExportDatabase { path, .. } = plan else {
            return Err(ServerError::Unsupported(
                "execute_export_database: wrong plan",
            ));
        };
        // EXPORT DATABASE writes a dump tree to a user-supplied server-side
        // path using the database process's own filesystem privileges, so —
        // exactly like server-side file COPY — it is restricted to superusers.
        self.ensure_database_dump_file_access()?;
        match self.txn_state {
            TxnState::Idle => {
                self.export_database_to_path(path)?;
                Ok(run_ddl_command("EXPORT DATABASE"))
            }
            TxnState::InTransaction(_) => Err(self.fail_if_in_transaction(
                ServerError::Unsupported("EXPORT DATABASE inside an explicit transaction block"),
            )),
            TxnState::Failed(_) => Err(ServerError::TransactionAborted),
        }
    }

    pub(crate) fn execute_import_database(
        &mut self,
        plan: &LogicalPlan,
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::ImportDatabase { path, .. } = plan else {
            return Err(ServerError::Unsupported(
                "execute_import_database: wrong plan",
            ));
        };
        // IMPORT DATABASE reads a dump tree from a user-supplied server-side
        // path using the database process's own filesystem privileges, so —
        // exactly like server-side file COPY — it is restricted to superusers.
        self.ensure_database_dump_file_access()?;
        match self.txn_state {
            TxnState::Idle => {
                self.import_database_from_path(path)?;
                Ok(run_ddl_command("IMPORT DATABASE"))
            }
            TxnState::InTransaction(_) => Err(self.fail_if_in_transaction(
                ServerError::Unsupported("IMPORT DATABASE inside an explicit transaction block"),
            )),
            TxnState::Failed(_) => Err(ServerError::TransactionAborted),
        }
    }

    /// Gate for server-side database dump file access (`EXPORT DATABASE` /
    /// `IMPORT DATABASE`). Both statements read or write a directory tree on
    /// the database host using the server process's own filesystem
    /// privileges, so — like server-side file COPY (see
    /// [`Session::ensure_copy_server_file_access`]) — they are restricted to
    /// superusers. Without this gate any role able to run the command could
    /// write attacker-controlled bytes anywhere the server can write (host
    /// takeover / exfiltration) or read another tenant's dump directory.
    fn ensure_database_dump_file_access(&self) -> Result<(), ServerError> {
        if self.current_role_is_superuser() {
            Ok(())
        } else {
            Err(ServerError::InsufficientPrivilege(
                "permission denied for server-side database dump file access: \
                 must be superuser to use EXPORT DATABASE / IMPORT DATABASE"
                    .to_owned(),
            ))
        }
    }

    fn export_database_to_path(&mut self, path: &str) -> Result<(), ServerError> {
        let destination = checked_dump_destination(path)?;
        let snapshot = self.state.catalog_snapshot();
        self.validate_exportable_catalog(&snapshot)?;

        let txn = self.state.txn_manager.begin(IsolationLevel::RepeatableRead);
        let staged = staged_dump_directory(&destination, self.pid)?;
        let outcome = self.write_dump_directory(&staged, &destination, &snapshot, &txn);
        let finalise = self.finalise_read_maintenance_transaction(
            txn,
            outcome,
            "EXPORT DATABASE snapshot commit",
            "EXPORT DATABASE snapshot rollback",
        );
        if let Err(err) = finalise {
            let _ = std::fs::remove_dir_all(&staged);
            return Err(err);
        }
        std::fs::rename(&staged, &destination)
            .map_err(|err| ServerError::ddl(format!("EXPORT DATABASE rename: {err}")))?;
        sync_parent_dir(&destination)?;
        Ok(())
    }

    fn import_database_from_path(&mut self, path: &str) -> Result<(), ServerError> {
        self.ensure_import_target_empty()?;
        let source = checked_dump_source(path)?;
        let (manifest, checksum_paths) = read_and_verify_dump(&source)?;
        validate_manifest(&manifest)?;
        validate_manifest_files(&manifest, &checksum_paths)?;

        let mut statements = Vec::new();
        collect_statement_slices(&source.join(&manifest.schema_file), &mut statements)?;
        for table in &manifest.tables {
            collect_statement_slices(&source.join(&table.data_file), &mut statements)?;
        }

        for stmt in &statements {
            self.validate_import_statement(stmt)?;
        }
        for stmt in statements {
            // Import replays statements internally; there is no network
            // driver to consume a streaming handle, so never request one.
            let _ = self.execute_query(&stmt, false)?;
        }
        Ok(())
    }

    fn write_dump_directory(
        &self,
        staged: &Path,
        destination: &Path,
        snapshot: &CatalogSnapshot,
        txn: &Transaction,
    ) -> Result<(), ServerError> {
        std::fs::create_dir(staged).map_err(|err| {
            ServerError::ddl(format!("EXPORT DATABASE create staging dir: {err}"))
        })?;
        std::fs::create_dir(staged.join(DATA_DIR))
            .map_err(|err| ServerError::ddl(format!("EXPORT DATABASE create data dir: {err}")))?;

        let tables = export_tables(snapshot);
        let indexes = export_indexes(snapshot, &tables)?;
        let sequences = export_sequences(self)?;
        let schemas = export_schemas(self, &tables, &sequences)?;
        let schema_sql = build_schema_sql(&schemas, &sequences, &tables, &indexes)?;
        let data_files = self.export_table_data(&tables, txn)?;

        let manifest = DumpManifest {
            format: DUMP_FORMAT.to_owned(),
            version: DUMP_VERSION,
            product_version: env!("CARGO_PKG_VERSION").to_owned(),
            schema_file: SCHEMA_FILE.to_owned(),
            schemas: schemas
                .iter()
                .map(|schema| DumpSchema {
                    name: schema.clone(),
                })
                .collect(),
            sequences: sequences
                .iter()
                .map(|sequence| DumpSequence {
                    schema: sequence.schema.clone(),
                    name: sequence.name.clone(),
                })
                .collect(),
            tables: data_files
                .iter()
                .map(|file| file.manifest_entry.clone())
                .collect(),
            indexes: indexes
                .iter()
                .map(|index| DumpIndex {
                    schema: index.schema_name.clone(),
                    name: index.name.clone(),
                    table_schema: tables
                        .iter()
                        .find(|table| table.oid == index.table_oid)
                        .map_or_else(String::new, |table| table.schema_name.clone()),
                    table_name: tables
                        .iter()
                        .find(|table| table.oid == index.table_oid)
                        .map_or_else(String::new, |table| table.name.clone()),
                })
                .collect(),
        };

        let mut checksums = Vec::new();
        write_dump_file(staged, SCHEMA_FILE, schema_sql.as_bytes(), &mut checksums)?;
        for file in data_files {
            write_dump_file(
                staged,
                &file.manifest_entry.data_file,
                &file.bytes,
                &mut checksums,
            )?;
        }

        let manifest_bytes = serde_json::to_vec_pretty(&manifest)
            .map_err(|err| ServerError::ddl(format!("EXPORT DATABASE manifest encode: {err}")))?;
        write_dump_file(staged, MANIFEST_FILE, &manifest_bytes, &mut checksums)?;
        let checksums_bytes = serde_json::to_vec_pretty(&DumpChecksums {
            algorithm: "sha256".to_owned(),
            files: checksums,
        })
        .map_err(|err| ServerError::ddl(format!("EXPORT DATABASE checksums encode: {err}")))?;
        write_file_sync(&staged.join(CHECKSUMS_FILE), &checksums_bytes)?;
        sync_dir(staged)?;
        sync_parent_dir(destination)?;
        Ok(())
    }

    fn validate_exportable_catalog(&self, snapshot: &CatalogSnapshot) -> Result<(), ServerError> {
        if !snapshot.enum_types.is_empty()
            || !snapshot.composite_types.is_empty()
            || !snapshot.domain_types.is_empty()
        {
            return Err(ServerError::unsupported(
                "EXPORT DATABASE does not yet support user-defined types",
            ));
        }
        if !self.state.materialized_views.is_empty() {
            return Err(ServerError::unsupported(
                "EXPORT DATABASE does not yet support materialized views",
            ));
        }
        if !self.state.regular_views.is_empty() {
            return Err(ServerError::unsupported(
                "EXPORT DATABASE does not yet support regular views",
            ));
        }
        for table in export_tables(snapshot) {
            if !table.options.is_empty() {
                return Err(ServerError::unsupported(format!(
                    "EXPORT DATABASE does not yet support storage options on table {}",
                    qualified_name(&table.schema_name, &table.name)
                )));
            }
            if snapshot
                .constraints
                .values()
                .any(|constraint| constraint.conrelid == table.oid)
            {
                return Err(ServerError::unsupported(format!(
                    "EXPORT DATABASE does not yet support table constraints on {}",
                    qualified_name(&table.schema_name, &table.name)
                )));
            }
            if let Some(runtime) = self.state.table_constraints.get(&table.oid) {
                let runtime = runtime.value();
                if runtime.defaults.iter().any(Option::is_some)
                    || runtime.sequence_defaults.iter().any(Option::is_some)
                    || runtime.identity_always.iter().any(|flag| *flag)
                    || runtime.generated_stored.iter().any(Option::is_some)
                    || !runtime.checks.is_empty()
                    || !runtime.foreign_keys.is_empty()
                    || !runtime.exclusion_constraints.is_empty()
                    || !runtime.indexes.is_empty()
                {
                    return Err(ServerError::unsupported(format!(
                        "EXPORT DATABASE does not yet support runtime metadata on table {}",
                        qualified_name(&table.schema_name, &table.name)
                    )));
                }
            }
            if let Some(row_security) = self.state.row_security.get(&table.oid)
                && (row_security.enabled || !row_security.policies.is_empty())
            {
                return Err(ServerError::unsupported(format!(
                    "EXPORT DATABASE does not yet support row security policies on table {}",
                    qualified_name(&table.schema_name, &table.name)
                )));
            }
            for field in table.schema.fields() {
                validate_exportable_type(&field.data_type, &table)?;
            }
        }
        Ok(())
    }

    fn export_table_data(
        &self,
        tables: &[TableEntry],
        txn: &Transaction,
    ) -> Result<Vec<ExportedDataFile>, ServerError> {
        let mut files = Vec::with_capacity(tables.len());
        for (ordinal, table) in tables.iter().enumerate() {
            let data_file = format!(
                "{DATA_DIR}/{:04}_{}_{}.sql",
                ordinal + 1,
                sanitize_file_component(&table.schema_name),
                sanitize_file_component(&table.name)
            );
            let (bytes, row_count) = self.export_one_table_data(table, txn)?;
            files.push(ExportedDataFile {
                manifest_entry: DumpTable {
                    schema: table.schema_name.clone(),
                    name: table.name.clone(),
                    data_file,
                    row_count,
                },
                bytes,
            });
        }
        Ok(files)
    }

    fn export_one_table_data(
        &self,
        table: &TableEntry,
        txn: &Transaction,
    ) -> Result<(Vec<u8>, u64), ServerError> {
        let rel = RelationId(table.oid);
        let block_count = self.state.heap.block_count(rel).max(table.n_blocks);
        let codec = ultrasql_executor::RowCodec::new(table.schema.clone());
        let mut out = Vec::new();
        let mut row_count = 0_u64;
        let target = qualified_name(&table.schema_name, &table.name);
        let columns = table
            .schema
            .fields()
            .iter()
            .map(|field| quote_ident(&field.name))
            .collect::<Vec<_>>()
            .join(", ");
        let scan = self.state.heap.scan_visible(
            rel,
            block_count,
            &txn.snapshot,
            self.state.txn_manager.as_ref(),
        );
        for tuple in scan {
            let tuple = tuple
                .map_err(|err| ServerError::ddl(format!("EXPORT DATABASE heap scan: {err}")))?;
            let row = codec
                .decode(&tuple.data)
                .map_err(|err| ServerError::ddl(format!("EXPORT DATABASE row decode: {err}")))?;
            let values = row
                .iter()
                .zip(table.schema.fields())
                .map(|(value, field)| sql_literal(value, &field.data_type))
                .collect::<Result<Vec<_>, _>>()?
                .join(", ");
            writeln!(
                &mut out,
                "INSERT INTO {target} ({columns}) VALUES ({values});"
            )
            .map_err(|err| ServerError::ddl(format!("EXPORT DATABASE write row: {err}")))?;
            row_count = row_count
                .checked_add(1)
                .ok_or_else(|| ServerError::ddl("EXPORT DATABASE row count overflow".to_owned()))?;
        }
        Ok((out, row_count))
    }

    fn ensure_import_target_empty(&self) -> Result<(), ServerError> {
        let snapshot = self.state.catalog_snapshot();
        if snapshot
            .tables
            .values()
            .any(|table| !is_system_schema(&table.schema_name))
        {
            return Err(ServerError::unsupported(
                "IMPORT DATABASE target database is not empty",
            ));
        }
        if self
            .state
            .sequence_namespaces
            .iter()
            .any(|entry| !is_system_schema(entry.value()))
        {
            return Err(ServerError::unsupported(
                "IMPORT DATABASE target database is not empty",
            ));
        }
        if self
            .state
            .schemas
            .iter()
            .any(|entry| !builtin_schema_name(entry.key()))
        {
            return Err(ServerError::unsupported(
                "IMPORT DATABASE target database is not empty",
            ));
        }
        Ok(())
    }

    fn validate_import_statement(&self, stmt: &str) -> Result<(), ServerError> {
        let parsed = Parser::new(stmt).parse_statement()?;
        match parsed {
            Statement::CreateSchema(_)
            | Statement::CreateSequence(_)
            | Statement::CreateTable(_)
            | Statement::CreateIndex(_)
            | Statement::Insert(_)
            | Statement::Select(_) => Ok(()),
            _ => Err(ServerError::unsupported(
                "IMPORT DATABASE dump contains a statement outside the portable dump subset",
            )),
        }
    }
}

#[derive(Clone, Debug)]
struct ExportSequence {
    schema: String,
    name: String,
    snapshot: SequenceSnapshot,
}

fn checked_dump_destination(path: &str) -> Result<PathBuf, ServerError> {
    let path = PathBuf::from(path);
    if path.as_os_str().is_empty() {
        return Err(ServerError::unsupported(
            "EXPORT DATABASE destination path cannot be empty",
        ));
    }
    if path.try_exists().map_err(ServerError::Io)? {
        return Err(ServerError::unsupported(
            "EXPORT DATABASE destination already exists; choose a new directory",
        ));
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if !parent.is_dir() {
        return Err(ServerError::unsupported(
            "EXPORT DATABASE destination parent must be an existing directory",
        ));
    }
    Ok(path)
}

fn checked_dump_source(path: &str) -> Result<PathBuf, ServerError> {
    let path = PathBuf::from(path);
    if path.as_os_str().is_empty() {
        return Err(ServerError::unsupported(
            "IMPORT DATABASE source path cannot be empty",
        ));
    }
    let metadata = std::fs::symlink_metadata(&path)
        .map_err(|err| ServerError::ddl(format!("IMPORT DATABASE source metadata: {err}")))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(ServerError::unsupported(
            "IMPORT DATABASE source must be a real dump directory",
        ));
    }
    Ok(path)
}

fn staged_dump_directory(destination: &Path, pid: u32) -> Result<PathBuf, ServerError> {
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let leaf = destination
        .file_name()
        .ok_or_else(|| {
            ServerError::unsupported("EXPORT DATABASE destination must name a directory")
        })?
        .to_string_lossy();
    let staged = parent.join(format!(".{leaf}.tmp-{pid}"));
    if staged.try_exists().map_err(ServerError::Io)? {
        return Err(ServerError::unsupported(
            "EXPORT DATABASE staging directory already exists; retry after removing the stale temp directory",
        ));
    }
    Ok(staged)
}

fn write_dump_file(
    root: &Path,
    relative: &str,
    bytes: &[u8],
    checksums: &mut Vec<DumpFileChecksum>,
) -> Result<(), ServerError> {
    validate_relative_dump_path(relative)?;
    let path = root.join(relative);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|err| ServerError::ddl(format!("EXPORT DATABASE create dir: {err}")))?;
    }
    write_file_sync(&path, bytes)?;
    checksums.push(DumpFileChecksum {
        path: relative.to_owned(),
        sha256: sha256_hex(bytes),
        bytes: u64::try_from(bytes.len())
            .map_err(|_| ServerError::ddl("EXPORT DATABASE file size overflow".to_owned()))?,
    });
    Ok(())
}

fn write_file_sync(path: &Path, bytes: &[u8]) -> Result<(), ServerError> {
    let mut file = File::create(path)
        .map_err(|err| ServerError::ddl(format!("write {}: {err}", path.display())))?;
    file.write_all(bytes)
        .map_err(|err| ServerError::ddl(format!("write {}: {err}", path.display())))?;
    ultrasql_core::fsync::full_fsync(&file)
        .map_err(|err| ServerError::ddl(format!("sync {}: {err}", path.display())))?;
    Ok(())
}

fn sync_parent_dir(path: &Path) -> Result<(), ServerError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    sync_dir(parent)
}

fn sync_dir(path: &Path) -> Result<(), ServerError> {
    sync_dir_inner(path)
        .map_err(|err| ServerError::ddl(format!("sync directory {}: {err}", path.display())))
}

#[cfg(unix)]
fn sync_dir_inner(path: &Path) -> io::Result<()> {
    let dir = File::open(path)?;
    match dir.sync_all() {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => Ok(()),
        Err(err) => Err(err),
    }
}

#[cfg(not(unix))]
fn sync_dir_inner(_path: &Path) -> io::Result<()> {
    Ok(())
}

fn read_and_verify_dump(root: &Path) -> Result<(DumpManifest, HashSet<String>), ServerError> {
    let checksums = read_json::<DumpChecksums>(&root.join(CHECKSUMS_FILE))?;
    if checksums.algorithm != "sha256" {
        return Err(ServerError::unsupported(
            "IMPORT DATABASE checksum file must use sha256",
        ));
    }
    let mut paths = HashSet::new();
    for entry in &checksums.files {
        validate_relative_dump_path(&entry.path)?;
        let bytes = read_file_bytes(&root.join(&entry.path))?;
        let actual_len = u64::try_from(bytes.len())
            .map_err(|_| ServerError::ddl("IMPORT DATABASE file size overflow".to_owned()))?;
        if actual_len != entry.bytes || sha256_hex(&bytes) != entry.sha256 {
            return Err(ServerError::unsupported(format!(
                "IMPORT DATABASE checksum mismatch for {}",
                entry.path
            )));
        }
        if !paths.insert(entry.path.clone()) {
            return Err(ServerError::unsupported(
                "IMPORT DATABASE checksum file contains duplicate paths",
            ));
        }
    }
    if !paths.contains(MANIFEST_FILE) {
        return Err(ServerError::unsupported(
            "IMPORT DATABASE checksum file does not cover manifest.json",
        ));
    }
    let manifest = read_json::<DumpManifest>(&root.join(MANIFEST_FILE))?;
    Ok((manifest, paths))
}

fn validate_manifest(manifest: &DumpManifest) -> Result<(), ServerError> {
    if manifest.format != DUMP_FORMAT {
        return Err(ServerError::unsupported(
            "IMPORT DATABASE manifest format is not an UltraSQL logical dump",
        ));
    }
    if manifest.version != DUMP_VERSION {
        return Err(ServerError::unsupported(format!(
            "IMPORT DATABASE dump version {} is not supported by this server",
            manifest.version
        )));
    }
    validate_relative_dump_path(&manifest.schema_file)?;
    if manifest.schema_file != SCHEMA_FILE {
        return Err(ServerError::unsupported(
            "IMPORT DATABASE manifest must use schema.sql as schema_file",
        ));
    }
    for table in &manifest.tables {
        validate_relative_dump_path(&table.data_file)?;
    }
    Ok(())
}

fn validate_manifest_files(
    manifest: &DumpManifest,
    checksum_paths: &HashSet<String>,
) -> Result<(), ServerError> {
    if !checksum_paths.contains(&manifest.schema_file) {
        return Err(ServerError::unsupported(
            "IMPORT DATABASE checksum file does not cover schema.sql",
        ));
    }
    for table in &manifest.tables {
        if !checksum_paths.contains(&table.data_file) {
            return Err(ServerError::unsupported(format!(
                "IMPORT DATABASE checksum file does not cover {}",
                table.data_file
            )));
        }
    }
    Ok(())
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, ServerError> {
    let bytes = read_file_bytes(path)?;
    serde_json::from_slice(&bytes)
        .map_err(|err| ServerError::unsupported(format!("IMPORT DATABASE invalid JSON: {err}")))
}

fn read_file_bytes(path: &Path) -> Result<Vec<u8>, ServerError> {
    let mut file = File::open(path)
        .map_err(|err| ServerError::ddl(format!("read {}: {err}", path.display())))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|err| ServerError::ddl(format!("read {}: {err}", path.display())))?;
    Ok(bytes)
}

fn collect_statement_slices(path: &Path, out: &mut Vec<String>) -> Result<(), ServerError> {
    let text = std::fs::read_to_string(path)
        .map_err(|err| ServerError::ddl(format!("read {}: {err}", path.display())))?;
    let slices = Parser::new(&text).parse_statement_slices()?;
    out.extend(slices.into_iter().map(str::to_owned));
    Ok(())
}

fn validate_relative_dump_path(path: &str) -> Result<(), ServerError> {
    let rel = Path::new(path);
    if rel.is_absolute() || path.is_empty() {
        return Err(ServerError::unsupported(
            "IMPORT DATABASE dump file paths must be non-empty relative paths",
        ));
    }
    for component in rel.components() {
        match component {
            Component::Normal(_) => {}
            _ => {
                return Err(ServerError::unsupported(
                    "IMPORT DATABASE dump file paths must not escape the dump directory",
                ));
            }
        }
    }
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn export_tables(snapshot: &CatalogSnapshot) -> Vec<TableEntry> {
    let mut tables = snapshot
        .tables
        .values()
        .filter(|table| !is_system_schema(&table.schema_name))
        .cloned()
        .collect::<Vec<_>>();
    tables.sort_by(|left, right| {
        left.schema_name
            .cmp(&right.schema_name)
            .then(left.name.cmp(&right.name))
    });
    tables
}

fn export_indexes(
    snapshot: &CatalogSnapshot,
    tables: &[TableEntry],
) -> Result<Vec<IndexEntry>, ServerError> {
    let table_oids = tables.iter().map(|table| table.oid).collect::<HashSet<_>>();
    let mut indexes = snapshot
        .indexes
        .values()
        .filter(|index| table_oids.contains(&index.table_oid))
        .cloned()
        .collect::<Vec<_>>();
    indexes.sort_by(|left, right| {
        left.schema_name
            .cmp(&right.schema_name)
            .then(left.name.cmp(&right.name))
    });
    for index in &indexes {
        if index.access_method != "btree" {
            return Err(ServerError::unsupported(format!(
                "EXPORT DATABASE does not yet support {} indexes",
                index.access_method
            )));
        }
        if index.opclasses.iter().any(Option::is_some)
            || !index.options.is_empty()
            || index.columns.is_empty()
        {
            return Err(ServerError::unsupported(format!(
                "EXPORT DATABASE does not yet support advanced index metadata on {}",
                qualified_name(&index.schema_name, &index.name)
            )));
        }
    }
    Ok(indexes)
}

fn export_sequences<RW>(session: &Session<RW>) -> Result<Vec<ExportSequence>, ServerError>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    let mut sequences = Vec::new();
    for item in session.state.sequences.iter() {
        let key = item.key().clone();
        let schema = session
            .state
            .sequence_namespaces
            .get(&key)
            .map_or_else(|| "public".to_owned(), |entry| entry.value().clone());
        if is_system_schema(&schema) {
            continue;
        }
        let owner = session.state.sequence_owners.get(&key).map_or_else(
            || session.current_user.clone(),
            |entry| entry.value().clone(),
        );
        if !owner.eq_ignore_ascii_case(&session.current_user) {
            return Err(ServerError::unsupported(format!(
                "EXPORT DATABASE cannot preserve owner {owner} for sequence {key}",
            )));
        }
        let name = key
            .rsplit_once('.')
            .map_or_else(|| key.clone(), |(_, name)| name.to_owned());
        sequences.push(ExportSequence {
            schema,
            name,
            snapshot: item.value().state_snapshot(),
        });
    }
    sequences.sort_by(|left, right| {
        left.schema
            .cmp(&right.schema)
            .then(left.name.cmp(&right.name))
    });
    Ok(sequences)
}

fn export_schemas<RW>(
    session: &Session<RW>,
    tables: &[TableEntry],
    sequences: &[ExportSequence],
) -> Result<Vec<String>, ServerError>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    let mut schemas = BTreeSet::new();
    for table in tables {
        if !builtin_schema_name(&table.schema_name) {
            schemas.insert(table.schema_name.clone());
        }
    }
    for sequence in sequences {
        if !builtin_schema_name(&sequence.schema) {
            schemas.insert(sequence.schema.clone());
        }
    }
    for item in session.state.schemas.iter() {
        if builtin_schema_name(item.key()) {
            continue;
        }
        if !item
            .value()
            .owner_role
            .eq_ignore_ascii_case(&session.current_user)
        {
            return Err(ServerError::unsupported(format!(
                "EXPORT DATABASE cannot preserve owner {} for schema {}",
                item.value().owner_role,
                item.key()
            )));
        }
        schemas.insert(item.key().clone());
    }
    Ok(schemas.into_iter().collect())
}

fn build_schema_sql(
    schemas: &[String],
    sequences: &[ExportSequence],
    tables: &[TableEntry],
    indexes: &[IndexEntry],
) -> Result<String, ServerError> {
    let table_by_oid = tables
        .iter()
        .map(|table| (table.oid, table))
        .collect::<BTreeMap<_, _>>();
    let mut out = String::new();
    writeln!(&mut out, "-- UltraSQL logical dump v{DUMP_VERSION}")
        .map_err(|err| ServerError::ddl(format!("EXPORT DATABASE schema write: {err}")))?;
    for schema in schemas {
        writeln!(&mut out, "CREATE SCHEMA {};", quote_ident(schema))
            .map_err(|err| ServerError::ddl(format!("EXPORT DATABASE schema write: {err}")))?;
    }
    for sequence in sequences {
        write_sequence_sql(&mut out, sequence)?;
    }
    for table in tables {
        write_table_sql(&mut out, table)?;
    }
    for index in indexes {
        let table = table_by_oid.get(&index.table_oid).ok_or_else(|| {
            ServerError::ddl(format!(
                "EXPORT DATABASE index {} references missing table oid {}",
                index.name, index.table_oid
            ))
        })?;
        write_index_sql(&mut out, index, table)?;
    }
    Ok(out)
}

fn write_sequence_sql(out: &mut String, sequence: &ExportSequence) -> Result<(), ServerError> {
    let snap = &sequence.snapshot;
    writeln!(
        out,
        "CREATE SEQUENCE {} START WITH {} INCREMENT BY {} MINVALUE {} MAXVALUE {} CACHE {} {};",
        qualified_name(&sequence.schema, &sequence.name),
        snap.start_value,
        snap.increment,
        snap.min_value,
        snap.max_value,
        snap.cache_size,
        if snap.cycle { "CYCLE" } else { "NO CYCLE" },
    )
    .map_err(|err| ServerError::ddl(format!("EXPORT DATABASE sequence write: {err}")))?;
    writeln!(
        out,
        "SELECT setval({}, {}, {});",
        quote_string(&crate::sequence_lookup_key(
            &sequence.schema,
            &sequence.name
        )),
        snap.last_value,
        if snap.is_called { "true" } else { "false" },
    )
    .map_err(|err| ServerError::ddl(format!("EXPORT DATABASE sequence write: {err}")))?;
    Ok(())
}

fn write_table_sql(out: &mut String, table: &TableEntry) -> Result<(), ServerError> {
    writeln!(
        out,
        "CREATE TABLE {} (",
        qualified_name(&table.schema_name, &table.name)
    )
    .map_err(|err| ServerError::ddl(format!("EXPORT DATABASE table write: {err}")))?;
    for (idx, field) in table.schema.fields().iter().enumerate() {
        let suffix = if idx + 1 == table.schema.len() {
            ""
        } else {
            ","
        };
        let nullable = if field.nullable { "" } else { " NOT NULL" };
        writeln!(
            out,
            "    {} {}{}{}",
            quote_ident(&field.name),
            field.data_type,
            nullable,
            suffix
        )
        .map_err(|err| ServerError::ddl(format!("EXPORT DATABASE table write: {err}")))?;
    }
    writeln!(out, ");")
        .map_err(|err| ServerError::ddl(format!("EXPORT DATABASE table write: {err}")))?;
    Ok(())
}

fn write_index_sql(
    out: &mut String,
    index: &IndexEntry,
    table: &TableEntry,
) -> Result<(), ServerError> {
    let columns = index
        .columns
        .iter()
        .map(|attnum| {
            let idx = usize::from(*attnum);
            table
                .schema
                .field(idx)
                .map(|field| quote_ident(&field.name))
                .ok_or_else(|| {
                    ServerError::ddl(format!(
                        "EXPORT DATABASE index {} references missing column {}",
                        index.name, idx
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    let unique = if index.is_unique { "UNIQUE " } else { "" };
    writeln!(
        out,
        "CREATE {unique}INDEX {} ON {} ({columns});",
        quote_ident(&index.name),
        qualified_name(&table.schema_name, &table.name)
    )
    .map_err(|err| ServerError::ddl(format!("EXPORT DATABASE index write: {err}")))?;
    Ok(())
}

fn validate_exportable_type(dtype: &DataType, table: &TableEntry) -> Result<(), ServerError> {
    match dtype {
        DataType::Enum { .. }
        | DataType::Composite { .. }
        | DataType::Domain { .. }
        | DataType::Record(_)
        | DataType::Null => Err(ServerError::unsupported(format!(
            "EXPORT DATABASE does not yet support type {dtype} on table {}",
            qualified_name(&table.schema_name, &table.name)
        ))),
        DataType::Array(inner) => validate_exportable_type(inner, table),
        _ => Ok(()),
    }
}

fn sql_literal(value: &Value, dtype: &DataType) -> Result<String, ServerError> {
    if matches!(value, Value::Null) {
        return Ok("NULL".to_owned());
    }
    match (dtype, value) {
        (_, Value::Bool(v)) => Ok(if *v { "TRUE" } else { "FALSE" }.to_owned()),
        (_, Value::Int16(v)) => Ok(v.to_string()),
        (DataType::Date, Value::Int32(v)) => Ok(cast_literal(&Value::Date(*v).to_string(), dtype)),
        (_, Value::Int32(v)) => Ok(v.to_string()),
        (DataType::Time, Value::Int64(v)) => Ok(cast_literal(&Value::Time(*v).to_string(), dtype)),
        (DataType::Timestamp, Value::Int64(v)) => {
            Ok(cast_literal(&Value::Timestamp(*v).to_string(), dtype))
        }
        (DataType::TimestampTz, Value::Int64(v)) => {
            Ok(cast_literal(&Value::TimestampTz(*v).to_string(), dtype))
        }
        (DataType::Money, Value::Int64(v)) => {
            Ok(cast_literal(&Value::Money(*v).to_string(), dtype))
        }
        (DataType::Decimal { scale, .. }, Value::Int64(v)) => Ok(Value::Decimal {
            value: i128::from(*v),
            scale: scale.unwrap_or(0),
        }
        .to_string()),
        (_, Value::Int64(v)) => Ok(v.to_string()),
        (_, Value::Oid(v) | Value::RegClass(v) | Value::RegType(v)) => Ok(v.raw().to_string()),
        (_, Value::PgLsn(_)) => Ok(cast_literal(&value.to_string(), dtype)),
        (_, Value::Float32(v)) if v.is_finite() => Ok(v.to_string()),
        (_, Value::Float64(v)) if v.is_finite() => Ok(v.to_string()),
        (_, Value::Float32(_) | Value::Float64(_)) => Err(ServerError::unsupported(
            "EXPORT DATABASE does not yet support non-finite floating-point values",
        )),
        _ => Ok(cast_literal(&value.to_string(), dtype)),
    }
}

fn cast_literal(text: &str, dtype: &DataType) -> String {
    format!("{}::{}", quote_string(text), dtype)
}

fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn quote_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn qualified_name(schema: &str, name: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(name))
}

fn sanitize_file_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}

fn is_system_schema(schema: &str) -> bool {
    matches!(schema, "pg_catalog" | "information_schema")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_dir_accepts_existing_directory() {
        let temp = tempfile::TempDir::new().expect("temp dir");

        sync_dir(temp.path()).expect("sync temp dir");
    }
}
