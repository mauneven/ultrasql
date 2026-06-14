# SET VARIABLE

`SET VARIABLE` assigns a session-local runtime parameter. It is a compatibility
spelling for UltraSQL's existing `SET name = value` session setting path.

## Syntax

```sql
SET VARIABLE name = value;
SET VARIABLE name TO value;
SET VARIABLE name = DEFAULT;
```

`name` may be a supported runtime parameter such as `statement_timeout` or a
dotted custom setting such as `ultrasql.tenant`.

## Examples

```sql
SET VARIABLE statement_timeout TO 50;
SHOW statement_timeout;

SET VARIABLE ultrasql.tenant = 'acme';
SHOW ultrasql.tenant;

SET VARIABLE statement_timeout = DEFAULT;
```

## Supported Behavior

- Scope is session-local. A setting made by one connection is not visible to
  another connection.
- Values use the same validation as ordinary `SET`: booleans, timeouts,
  `DateStyle`, `TimeZone`, and other supported parameters keep their existing
  checks.
- `DEFAULT` resets the parameter through the existing reset path.
- Prepared execution without parameters is supported by the extended-query
  protocol.

## Limitations

- `SET LOCAL VARIABLE` and `SET SESSION VARIABLE` are rejected. Use
  `SET VARIABLE name = value` for this spelling.
- Parameterized prepared statements such as `SET VARIABLE x = $1` are not
  supported because `SET` values are bound before parameter substitution.
- Undotted names must be known runtime parameters. Dotted custom settings are
  accepted and stored in the session settings map.
- `RESET VARIABLE name` is not a syntax form; use `SET VARIABLE name = DEFAULT`
  or the existing `RESET name`.

## Evidence

- Parser and AST: `crates/ultrasql-parser/src/statements/set_stmt.rs`,
  `crates/ultrasql-parser/src/ast.rs`.
- Binder and logical plan: `crates/ultrasql-planner/src/binder/mod.rs`,
  `crates/ultrasql-planner/src/plan.rs`.
- Server execution: `crates/ultrasql-server/src/session/execute.rs`,
  `crates/ultrasql-server/src/session/ext.rs`.
- Tests: `crates/ultrasql-parser/src/statements/set_stmt.rs`,
  `crates/ultrasql-planner/src/binder/tests.rs`,
  `crates/ultrasql-server/tests/set_variable_round_trip.rs`.
