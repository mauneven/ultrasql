//! Session-local sequence and advisory-lock state.
//!
//! Moved verbatim from the crate root; behavior unchanged.
use super::*;

pub(crate) fn deferred_fk_key(row: &[Value], columns: &[usize]) -> Option<Vec<Value>> {
    let mut key = Vec::with_capacity(columns.len());
    for &idx in columns {
        let value = row.get(idx)?;
        if matches!(value, Value::Null) {
            return None;
        }
        key.push(value.clone());
    }
    Some(key)
}

/// Session-local sequence state shared with sequence-backed defaults.
#[derive(Clone, Debug, Default)]
pub struct SequenceSessionState {
    currvals: Arc<parking_lot::Mutex<std::collections::HashMap<String, i64>>>,
    last_sequence: Arc<parking_lot::Mutex<Option<String>>>,
}

#[derive(Clone, Debug)]
pub(crate) struct SequenceSessionSnapshot {
    currvals: std::collections::HashMap<String, i64>,
    last_sequence: Option<String>,
}

impl SequenceSessionState {
    /// Record a generated value for `currval` / `lastval`.
    pub fn record_nextval(&self, name: &str, value: i64) {
        let folded = name.to_ascii_lowercase();
        self.currvals.lock().insert(folded.clone(), value);
        *self.last_sequence.lock() = Some(folded);
    }

    /// Drop session-local state for a removed sequence.
    pub fn forget(&self, name: &str) {
        let folded = name.to_ascii_lowercase();
        self.currvals.lock().remove(&folded);
        if self.last_sequence.lock().as_deref() == Some(folded.as_str()) {
            *self.last_sequence.lock() = None;
        }
    }

    pub(crate) fn snapshot(&self) -> SequenceSessionSnapshot {
        SequenceSessionSnapshot {
            currvals: self.currvals.lock().clone(),
            last_sequence: self.last_sequence.lock().clone(),
        }
    }

    pub(crate) fn restore_snapshot(&self, snapshot: SequenceSessionSnapshot) {
        *self.currvals.lock() = snapshot.currvals;
        *self.last_sequence.lock() = snapshot.last_sequence;
    }

    /// Return the session-local value for a named sequence.
    pub fn currval(&self, name: &str) -> Option<i64> {
        self.currvals
            .lock()
            .get(&name.to_ascii_lowercase())
            .copied()
    }

    /// Return the most recent sequence/value pair in this session.
    pub fn lastval(&self) -> Option<(String, i64)> {
        let name = self.last_sequence.lock().clone()?;
        let value = self.currvals.lock().get(&name).copied()?;
        Some((name, value))
    }
}

/// Session-local ownership for advisory locks.
#[derive(Clone, Debug)]
pub struct AdvisorySessionState {
    owner: Xid,
    held: Arc<parking_lot::Mutex<std::collections::HashMap<LockTag, usize>>>,
}

impl AdvisorySessionState {
    /// Build a stable advisory-lock owner for one server session.
    #[must_use]
    pub fn new(pid: u32) -> Self {
        Self {
            owner: Xid::new(u64::MAX.saturating_sub(u64::from(pid))),
            held: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Evaluate a PostgreSQL advisory-lock function against this session.
    ///
    /// `lock_wait` bounds the blocking `pg_advisory_lock` wait: the
    /// session's `lock_timeout` expiry surfaces as SQLSTATE `55P03` and a
    /// statement-timeout / client cancel as `57014`, matching PostgreSQL
    /// (`lock_timeout` applies to advisory locks too).
    pub fn evaluate_function(
        &self,
        name: &str,
        args: &[Value],
        lock_manager: &LockManager,
        lock_wait: &ultrasql_txn::LockWait,
    ) -> Result<Value, ServerError> {
        match name {
            "pg_advisory_lock" => {
                let Some(tag) = advisory_tag_from_values(name, args)? else {
                    return Ok(Value::Null);
                };
                self.lock(tag, lock_manager, name, lock_wait)?;
                Ok(Value::Null)
            }
            "pg_try_advisory_lock" => {
                let Some(tag) = advisory_tag_from_values(name, args)? else {
                    return Ok(Value::Null);
                };
                Ok(Value::Bool(self.try_lock(tag, lock_manager, name)?))
            }
            "pg_advisory_unlock" => {
                let Some(tag) = advisory_tag_from_values(name, args)? else {
                    return Ok(Value::Null);
                };
                Ok(Value::Bool(self.unlock(tag, lock_manager)))
            }
            "pg_advisory_unlock_all" => {
                if !args.is_empty() {
                    return Err(advisory_type_error(format!(
                        "{name}: expected 0 arguments, got {}",
                        args.len()
                    )));
                }
                self.release_all(lock_manager);
                Ok(Value::Null)
            }
            _ => Err(ServerError::Unsupported("advisory lock function")),
        }
    }

    /// Evaluate a PostgreSQL transaction-scoped advisory-lock function.
    pub fn evaluate_transaction_function(
        &self,
        name: &str,
        args: &[Value],
        lock_manager: &LockManager,
        owner: Xid,
    ) -> Result<Value, ServerError> {
        match name {
            "pg_try_advisory_xact_lock" => {
                let Some(tag) = advisory_tag_from_values(name, args)? else {
                    return Ok(Value::Null);
                };
                let acquired = lock_manager
                    .try_acquire(LockRequest {
                        xid: owner,
                        tag,
                        mode: LockMode::Exclusive,
                    })
                    .map_err(|err| advisory_type_error(format!("{name}: {err}")))?;
                Ok(Value::Bool(acquired))
            }
            _ => Err(ServerError::Unsupported(
                "transaction advisory lock function",
            )),
        }
    }

    /// Release every advisory lock held by this session.
    pub fn release_all(&self, lock_manager: &LockManager) {
        let tags: Vec<LockTag> = {
            let mut held = self.held.lock();
            let tags = held.keys().copied().collect();
            held.clear();
            tags
        };
        for tag in tags {
            lock_manager.release(self.owner, tag, LockMode::Exclusive);
        }
    }

    fn lock(
        &self,
        tag: LockTag,
        lock_manager: &LockManager,
        name: &str,
        lock_wait: &ultrasql_txn::LockWait,
    ) -> Result<(), ServerError> {
        {
            let mut held = self.held.lock();
            if let Some(count) = held.get_mut(&tag) {
                *count = count.saturating_add(1);
                return Ok(());
            }
        }
        lock_manager
            .acquire_with_wait(
                LockRequest {
                    xid: self.owner,
                    tag,
                    mode: LockMode::Exclusive,
                },
                lock_wait,
            )
            .map_err(|err| match err {
                ultrasql_txn::LockError::Timeout => {
                    ServerError::LockNotAvailable(crate::txn_exec::LOCK_TIMEOUT_MESSAGE.to_owned())
                }
                ultrasql_txn::LockError::Cancelled => {
                    ServerError::Execute(ultrasql_executor::ExecError::Cancelled)
                }
                other => advisory_type_error(format!("{name}: {other}")),
            })?;
        self.held.lock().insert(tag, 1);
        Ok(())
    }

    fn try_lock(
        &self,
        tag: LockTag,
        lock_manager: &LockManager,
        name: &str,
    ) -> Result<bool, ServerError> {
        {
            let mut held = self.held.lock();
            if let Some(count) = held.get_mut(&tag) {
                *count = count.saturating_add(1);
                return Ok(true);
            }
        }
        let acquired = lock_manager
            .try_acquire(LockRequest {
                xid: self.owner,
                tag,
                mode: LockMode::Exclusive,
            })
            .map_err(|err| advisory_type_error(format!("{name}: {err}")))?;
        if acquired {
            self.held.lock().insert(tag, 1);
        }
        Ok(acquired)
    }

    fn unlock(&self, tag: LockTag, lock_manager: &LockManager) -> bool {
        let should_release = {
            let mut held = self.held.lock();
            let Some(count) = held.get_mut(&tag) else {
                return false;
            };
            if *count > 1 {
                *count -= 1;
                false
            } else {
                held.remove(&tag);
                true
            }
        };
        if should_release {
            lock_manager.release(self.owner, tag, LockMode::Exclusive);
        }
        true
    }
}

pub(crate) fn advisory_tag_from_values(
    name: &str,
    args: &[Value],
) -> Result<Option<LockTag>, ServerError> {
    match args.len() {
        1 => {
            let Some(key) = advisory_i64_arg(name, args, 0)? else {
                return Ok(None);
            };
            let raw = u64::from_ne_bytes(key.to_ne_bytes());
            Ok(Some(LockTag::Advisory {
                classid: u32::try_from(raw >> 32)
                    .map_err(|_| advisory_type_error(format!("{name}: key high bits overflow")))?,
                objid: u32::try_from(raw & u64::from(u32::MAX))
                    .map_err(|_| advisory_type_error(format!("{name}: key low bits overflow")))?,
            }))
        }
        2 => {
            let Some(classid) = advisory_i32_arg(name, args, 0)? else {
                return Ok(None);
            };
            let Some(objid) = advisory_i32_arg(name, args, 1)? else {
                return Ok(None);
            };
            Ok(Some(LockTag::Advisory {
                classid: u32::from_ne_bytes(classid.to_ne_bytes()),
                objid: u32::from_ne_bytes(objid.to_ne_bytes()),
            }))
        }
        len => Err(advisory_type_error(format!(
            "{name}: expected 1 or 2 arguments, got {len}"
        ))),
    }
}

pub(crate) fn advisory_i64_arg(
    name: &str,
    args: &[Value],
    idx: usize,
) -> Result<Option<i64>, ServerError> {
    match args.get(idx) {
        Some(Value::Int16(value)) => Ok(Some(i64::from(*value))),
        Some(Value::Int32(value)) => Ok(Some(i64::from(*value))),
        Some(Value::Int64(value)) => Ok(Some(*value)),
        Some(Value::Null) => Ok(None),
        Some(other) => Err(advisory_type_error(format!(
            "{name}: argument {} must be integer, got {:?}",
            idx + 1,
            other.data_type()
        ))),
        None => Err(advisory_type_error(format!(
            "{name}: missing argument {}",
            idx + 1
        ))),
    }
}

pub(crate) fn advisory_i32_arg(
    name: &str,
    args: &[Value],
    idx: usize,
) -> Result<Option<i32>, ServerError> {
    let Some(value) = advisory_i64_arg(name, args, idx)? else {
        return Ok(None);
    };
    i32::try_from(value)
        .map(Some)
        .map_err(|_| advisory_type_error(format!("{name}: argument {} out of int4 range", idx + 1)))
}

pub(crate) fn advisory_type_error(message: String) -> ServerError {
    ServerError::Execute(ExecError::TypeMismatch(message))
}
