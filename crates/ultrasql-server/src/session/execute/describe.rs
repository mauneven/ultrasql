//! DESCRIBE handling, row-description encoding, and session-variable SET/SHOW/RESET.

use super::*;

impl<RW> Session<RW>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn execute_describe(
        &self,
        plan: &LogicalPlan,
        include_row_description: bool,
        result_formats: &[i16],
    ) -> Result<SelectResult, ServerError> {
        let LogicalPlan::Describe { target, schema } = plan else {
            return Err(ServerError::Unsupported("execute_describe: wrong plan"));
        };

        if let LogicalDescribeTarget::Object {
            name,
            namespace,
            kind: LogicalDescribeObjectKind::View,
            ..
        } = target
        {
            let key = ultrasql_catalog::table_lookup_key(namespace, name);
            let Some(entry) = self.state.persistent_catalog.lookup_table(&key) else {
                return Err(ServerError::ddl(format!("{key} is not a view")));
            };
            if !crate::is_regular_view_entry(&entry) {
                return Err(ServerError::ddl(format!("{key} is not a view")));
            }
        }

        let (fields, source_schema, source_object, source_kind) = match target {
            LogicalDescribeTarget::Object {
                name,
                namespace,
                kind,
                object_schema,
            } => {
                let key = ultrasql_catalog::table_lookup_key(namespace, name);
                // Overlay-aware so DESCRIBE of a table this session created in
                // its open transaction resolves it (self-visibility).
                let is_view = self.state.regular_views.contains_key(&key)
                    || self
                        .effective_catalog_snapshot()
                        .tables
                        .get(&key)
                        .is_some_and(crate::is_regular_view_entry);
                let source_kind = match kind {
                    LogicalDescribeObjectKind::Any => {
                        if is_view {
                            "view"
                        } else {
                            "table"
                        }
                    }
                    LogicalDescribeObjectKind::Table => {
                        if is_view {
                            return Err(ServerError::ddl(format!(
                                "{key} is a view; use DESCRIBE VIEW"
                            )));
                        }
                        "table"
                    }
                    LogicalDescribeObjectKind::View => {
                        if !is_view {
                            return Err(ServerError::ddl(format!("{key} is not a view")));
                        }
                        "view"
                    }
                };
                (
                    object_schema.fields(),
                    namespace.as_str(),
                    name.as_str(),
                    source_kind,
                )
            }
            LogicalDescribeTarget::Query { query_schema } => {
                (query_schema.fields(), "", "", "query")
            }
        };

        let mut messages = Vec::with_capacity(fields.len().saturating_add(2));
        if include_row_description {
            messages.push(Self::describe_row_description(schema, result_formats));
        }
        for field in fields {
            let nullable_format = Self::describe_result_format(&DataType::Bool, result_formats, 2);
            messages.push(BackendMessage::DataRow {
                columns: vec![
                    Some(field.name.clone().into_bytes()),
                    Some(field.data_type.to_string().into_bytes()),
                    Some(Self::describe_bool_cell(field.nullable, nullable_format)),
                    Some(source_schema.as_bytes().to_vec()),
                    Some(source_object.as_bytes().to_vec()),
                    Some(source_kind.as_bytes().to_vec()),
                ],
            });
        }
        let rows = u64::try_from(fields.len()).map_err(|_| {
            ServerError::Execute(ultrasql_executor::ExecError::NumericFieldOverflow(
                "DESCRIBE row count overflow".to_owned(),
            ))
        })?;
        messages.push(BackendMessage::CommandComplete {
            tag: format!("SELECT {rows}"),
        });
        Ok(SelectResult {
            messages,
            streamed_body: None,
            shared_streamed_body: None,
            streaming: None,
            rows,
        })
    }

    fn describe_row_description(schema: &Schema, result_formats: &[i16]) -> BackendMessage {
        let fields = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                let (type_oid, type_size) = match field.data_type {
                    DataType::Bool => (PG_OID_BOOL, 1),
                    _ => (PG_OID_TEXT, -1),
                };
                FieldDescription {
                    name: field.name.clone(),
                    table_oid: 0,
                    col_attnum: 0,
                    type_oid,
                    type_size,
                    type_modifier: -1,
                    format_code: Self::describe_result_format(
                        &field.data_type,
                        result_formats,
                        idx,
                    ),
                }
            })
            .collect();
        BackendMessage::RowDescription { fields }
    }

    fn describe_result_format(data_type: &DataType, result_formats: &[i16], idx: usize) -> i16 {
        match result_formats.len() {
            0 => match data_type {
                DataType::Float32 | DataType::Float64 => 1,
                _ => FORMAT_TEXT,
            },
            1 => result_formats[0],
            _ => result_formats.get(idx).copied().unwrap_or(FORMAT_TEXT),
        }
    }

    fn describe_bool_cell(value: bool, format: i16) -> Vec<u8> {
        if format == 1 {
            vec![u8::from(value)]
        } else if value {
            b"t".to_vec()
        } else {
            b"f".to_vec()
        }
    }

    pub(crate) fn execute_set_variable_reset(
        &mut self,
        name: &str,
    ) -> Result<SelectResult, ServerError> {
        match name {
            "jit" => {
                self.jit_enabled = false;
                Ok(result_encoder::run_ddl_command("RESET"))
            }
            "jit_above_cost" => {
                self.jit_above_rows = ultrasql_vec::jit::DEFAULT_JIT_ABOVE_ROWS;
                Ok(result_encoder::run_ddl_command("RESET"))
            }
            "statement_timeout" => {
                // RESET restores the server-wide default (PostgreSQL
                // semantics: back to the configured default, not 0).
                self.statement_timeout_ms = self.state.default_statement_timeout_ms;
                self.session_settings.remove("statement_timeout");
                Ok(result_encoder::run_ddl_command("RESET"))
            }
            "idle_in_transaction_session_timeout" => {
                self.idle_in_transaction_session_timeout_ms = 0;
                self.session_settings
                    .remove("idle_in_transaction_session_timeout");
                Ok(result_encoder::run_ddl_command("RESET"))
            }
            "work_mem" => {
                // Drop the override; the lowering path falls back to
                // DEFAULT_WORK_MEM_BYTES when the key is absent.
                self.session_settings.remove("work_mem");
                Ok(result_encoder::run_ddl_command("RESET"))
            }
            "extra_float_digits" => {
                self.session_settings.remove("extra_float_digits");
                Ok(result_encoder::run_ddl_command("RESET"))
            }
            "application_name" => {
                self.session_settings.remove("application_name");
                self.state
                    .workload_recorder
                    .update_session_application_name(self.pid, None);
                Ok(result_encoder::run_ddl_command("RESET"))
            }
            "client_min_messages" => {
                self.session_settings.remove("client_min_messages");
                Ok(result_encoder::run_ddl_command("RESET"))
            }
            "client_encoding" => Ok(result_encoder::run_ddl_command("RESET")),
            "datestyle" => {
                self.session_settings.remove("datestyle");
                Ok(result_encoder::run_ddl_command("RESET"))
            }
            "search_path" => {
                self.session_settings.remove("search_path");
                self.plan_cache_invalidate();
                Ok(result_encoder::run_ddl_command("RESET"))
            }
            "intervalstyle" => {
                self.session_settings.remove("intervalstyle");
                Ok(result_encoder::run_ddl_command("RESET"))
            }
            "lc_monetary" => {
                self.session_settings.remove("lc_monetary");
                Ok(result_encoder::run_ddl_command("RESET"))
            }
            "timezone" => {
                self.session_settings.remove("timezone");
                Ok(result_encoder::run_ddl_command("RESET"))
            }
            "synchronous_commit" => Ok(result_encoder::run_ddl_command("RESET")),
            _ if name.contains('.') => {
                self.session_settings.remove(&name.to_ascii_lowercase());
                Ok(result_encoder::run_ddl_command("RESET"))
            }
            _ => Err(ServerError::Unsupported("unsupported runtime parameter")),
        }
    }

    pub(crate) fn apply_session_variable(
        &mut self,
        name: &str,
        value: &str,
    ) -> Result<(), ServerError> {
        match name {
            "jit" => {
                self.jit_enabled = parse_bool_guc(value)?;
                Ok(())
            }
            "jit_above_cost" => {
                let parsed = value
                    .parse::<usize>()
                    .map_err(|_| ServerError::Unsupported("invalid jit_above_cost"))?;
                self.jit_above_rows = parsed;
                Ok(())
            }
            "statement_timeout" => {
                let parsed = parse_statement_timeout_ms(value)?;
                self.statement_timeout_ms = parsed;
                self.session_settings
                    .insert("statement_timeout".to_owned(), parsed.to_string());
                Ok(())
            }
            "idle_in_transaction_session_timeout" => {
                let parsed = parse_statement_timeout_ms(value)?;
                self.idle_in_transaction_session_timeout_ms = parsed;
                self.session_settings.insert(
                    "idle_in_transaction_session_timeout".to_owned(),
                    parsed.to_string(),
                );
                Ok(())
            }
            // Per-statement work-memory budget. Stored canonically as a byte
            // count so the lowering hot path (`Session::work_mem_budget`) can
            // parse it with a bare `u64::parse` and arm `WorkMemBudget`. A low
            // value makes sort / GROUP BY / hash-join spill to disk sooner.
            "work_mem" => {
                let parsed = parse_work_mem_bytes(value)?;
                self.session_settings
                    .insert("work_mem".to_owned(), parsed.to_string());
                Ok(())
            }
            // pgvector-compatible per-session HNSW exploration budget. Higher
            // ef_search trades latency for recall; the vector lowering reads it
            // from session_settings, so plans must be re-lowered after a change.
            "hnsw.ef_search" => {
                let parsed = value
                    .parse::<usize>()
                    .map_err(|_| ServerError::Unsupported("invalid hnsw.ef_search"))?;
                if parsed == 0 {
                    return Err(ServerError::Unsupported("hnsw.ef_search must be positive"));
                }
                self.session_settings
                    .insert("hnsw.ef_search".to_owned(), parsed.to_string());
                self.plan_cache_invalidate();
                Ok(())
            }
            "extra_float_digits" => {
                let parsed = value
                    .parse::<i32>()
                    .map_err(|_| ServerError::Unsupported("invalid extra_float_digits"))?;
                if !(-15..=3).contains(&parsed) {
                    return Err(ServerError::Unsupported("invalid extra_float_digits"));
                }
                self.session_settings
                    .insert("extra_float_digits".to_owned(), parsed.to_string());
                Ok(())
            }
            "application_name" => {
                self.session_settings
                    .insert("application_name".to_owned(), value.to_owned());
                self.state
                    .workload_recorder
                    .update_session_application_name(self.pid, Some(value.to_owned()));
                Ok(())
            }
            "client_min_messages" => match value.to_ascii_lowercase().as_str() {
                "debug5" | "debug4" | "debug3" | "debug2" | "debug1" | "log" | "notice"
                | "warning" | "error" => {
                    self.session_settings
                        .insert("client_min_messages".to_owned(), value.to_ascii_lowercase());
                    Ok(())
                }
                _ => Err(ServerError::Unsupported("invalid client_min_messages")),
            },
            "client_encoding" => match value.to_ascii_lowercase().as_str() {
                "utf8" | "utf-8" | "unicode" => Ok(()),
                _ => Err(ServerError::Unsupported("invalid client_encoding")),
            },
            "datestyle" => {
                let normalized = normalize_datestyle(value)?;
                self.session_settings
                    .insert("datestyle".to_owned(), normalized);
                Ok(())
            }
            "search_path" => {
                self.session_settings
                    .insert("search_path".to_owned(), value.to_owned());
                self.plan_cache_invalidate();
                Ok(())
            }
            "intervalstyle" => match value.to_ascii_lowercase().as_str() {
                "postgres" | "postgres_verbose" | "sql_standard" | "iso_8601" => {
                    self.session_settings
                        .insert("intervalstyle".to_owned(), value.to_ascii_lowercase());
                    Ok(())
                }
                _ => Err(ServerError::Unsupported("invalid intervalstyle")),
            },
            "lc_monetary" => {
                self.session_settings
                    .insert("lc_monetary".to_owned(), value.to_owned());
                Ok(())
            }
            "timezone" => {
                let normalized = value.trim();
                if normalized.is_empty() || timestamptz_display_in_timezone(0, normalized).is_none()
                {
                    return Err(ServerError::Unsupported("invalid timezone"));
                }
                self.session_settings
                    .insert("timezone".to_owned(), normalized.to_owned());
                Ok(())
            }
            "standard_conforming_strings" => match value.to_ascii_lowercase().as_str() {
                "on" => Ok(()),
                _ => Err(ServerError::Unsupported(
                    "invalid standard_conforming_strings",
                )),
            },
            "synchronous_commit" => match value.to_ascii_lowercase().as_str() {
                "on" | "off" | "local" | "remote_write" | "remote_apply" => Ok(()),
                _ => Err(ServerError::Unsupported("invalid synchronous_commit")),
            },
            _ if name.contains('.') => {
                self.session_settings
                    .insert(name.to_ascii_lowercase(), value.to_owned());
                Ok(())
            }
            _ => Err(ServerError::Unsupported("unsupported runtime parameter")),
        }
    }

    pub(crate) fn show_session_variable(
        &self,
        name: &str,
        include_row_description: bool,
    ) -> Result<SelectResult, ServerError> {
        let shown = match name {
            "jit" => {
                if self.jit_enabled {
                    "on".to_owned()
                } else {
                    "off".to_owned()
                }
            }
            "jit_above_cost" => self.jit_above_rows.to_string(),
            "statement_timeout" => self.statement_timeout_ms.to_string(),
            "idle_in_transaction_session_timeout" => {
                self.idle_in_transaction_session_timeout_ms.to_string()
            }
            "work_mem" => self
                .session_settings
                .get("work_mem")
                .cloned()
                .unwrap_or_else(|| DEFAULT_WORK_MEM_BYTES.to_string()),
            "hnsw.ef_search" => self
                .session_settings
                .get("hnsw.ef_search")
                .cloned()
                .unwrap_or_else(|| "auto".to_owned()),
            "extra_float_digits" => self
                .session_settings
                .get("extra_float_digits")
                .cloned()
                .unwrap_or_else(|| "1".to_owned()),
            "application_name" => self
                .session_settings
                .get("application_name")
                .cloned()
                .unwrap_or_default(),
            "client_encoding" => "UTF8".to_owned(),
            "client_min_messages" => self
                .session_settings
                .get("client_min_messages")
                .cloned()
                .unwrap_or_else(|| "notice".to_owned()),
            "datestyle" => self
                .session_settings
                .get("datestyle")
                .cloned()
                .unwrap_or_else(|| "ISO, MDY".to_owned()),
            "intervalstyle" => self
                .session_settings
                .get("intervalstyle")
                .cloned()
                .unwrap_or_else(|| "postgres".to_owned()),
            "lc_monetary" => self
                .session_settings
                .get("lc_monetary")
                .cloned()
                .unwrap_or_else(|| "C".to_owned()),
            "max_identifier_length" => "63".to_owned(),
            "server_version" => crate::REPORTED_SERVER_VERSION.to_owned(),
            "server_version_num" => "140000".to_owned(),
            "search_path" => self
                .session_settings
                .get("search_path")
                .cloned()
                .unwrap_or_else(|| "\"$user\", public".to_owned()),
            "timezone" | "TimeZone" => self
                .session_settings
                .get("timezone")
                .cloned()
                .unwrap_or_else(|| "UTC".to_owned()),
            "transaction_isolation" => shown_transaction_isolation(&self.txn_state).to_owned(),
            "standard_conforming_strings" => "on".to_owned(),
            "synchronous_commit" => "on".to_owned(),
            _ if name.contains('.') => self
                .session_settings
                .get(&name.to_ascii_lowercase())
                .cloned()
                .unwrap_or_default(),
            _ => return Err(ServerError::Unsupported("unsupported runtime parameter")),
        };
        let mut messages = Vec::with_capacity(3);
        if include_row_description {
            messages.push(BackendMessage::RowDescription {
                fields: vec![FieldDescription {
                    name: name.to_owned(),
                    table_oid: 0,
                    col_attnum: 0,
                    type_oid: 25,
                    type_size: -1,
                    type_modifier: -1,
                    format_code: 0,
                }],
            });
        }
        messages.push(BackendMessage::DataRow {
            columns: vec![Some(shown.into_bytes())],
        });
        messages.push(BackendMessage::CommandComplete {
            tag: "SHOW".to_owned(),
        });
        Ok(SelectResult {
            messages,
            streamed_body: None,
            shared_streamed_body: None,
            streaming: None,
            rows: 1,
        })
    }
}
