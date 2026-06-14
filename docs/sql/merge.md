# MERGE INTO

`MERGE INTO` applies ordered update, delete, and insert branches by comparing a
target table with a source relation.

## Syntax

```sql
MERGE INTO target_table [ AS target_alias ]
USING source_table_or_query [ AS source_alias ]
ON search_condition
WHEN MATCHED [ AND branch_condition ] THEN UPDATE SET column = expression [, ...]
WHEN MATCHED [ AND branch_condition ] THEN DELETE
WHEN NOT MATCHED [ AND branch_condition ] THEN INSERT [ (column [, ...]) ]
  VALUES (expression [, ...]);
```

At least one `WHEN` clause is required. Clauses are tested in statement order;
the first matching clause is the action for that source/target pair or source
row.

## Examples

```sql
CREATE TABLE merge_inventory (
  sku INT PRIMARY KEY,
  qty INT NOT NULL,
  active BOOL NOT NULL
);

CREATE TABLE merge_feed (
  sku INT,
  delta INT,
  retire BOOL
);

MERGE INTO merge_inventory AS target
USING merge_feed AS source
ON target.sku = source.sku
WHEN MATCHED AND source.retire THEN DELETE
WHEN MATCHED THEN UPDATE SET qty = target.qty + source.delta
WHEN NOT MATCHED THEN INSERT (sku, qty, active)
  VALUES (source.sku, source.delta, true);
```

## Supported Behavior

- Targets must be ordinary tables.
- The source may be a table or a subquery accepted by the normal `FROM`
  binder.
- `ON` and `WHEN ... AND` predicates must evaluate to `boolean` or `NULL`.
  `true` matches; `false` and `NULL` do not match.
- `WHEN MATCHED` supports `UPDATE SET` and `DELETE`.
- `WHEN NOT MATCHED` supports `INSERT [(columns)] VALUES (...)`.
- Branches execute as one statement. Constraint, index, or expression errors
  roll back all effects of the statement.
- Multiple source rows matching the same target row are rejected
  deterministically before target mutation.
- Existing index, generated-column, foreign-key, exclusion, default, and
  `NOT NULL` checks use the same DML paths as `INSERT`, `UPDATE`, and `DELETE`.

## Limitations

- `RETURNING`, `DO NOTHING`, `WHEN NOT MATCHED BY SOURCE`, and `DELETE WHERE`
  subclauses are not supported.
- MERGE on partitioned tables is rejected.
- MERGE with row-level security policies is rejected until policy checks are
  integrated for all branch types.
- The current matcher materializes source and target rows once and uses a
  nested-loop comparison. Cost is proportional to target rows times source
  rows.
- A statement that deletes a row and inserts the same unique key in another
  branch may still be rejected by uniqueness prechecks before the delete is
  applied.

## Evidence

- Parser and AST: `crates/ultrasql-parser/src/statements/merge.rs`,
  `crates/ultrasql-parser/src/ast.rs`.
- Binder and logical plan: `crates/ultrasql-planner/src/binder/dml.rs`,
  `crates/ultrasql-planner/src/plan.rs`.
- Optimizer and metadata plumbing: `crates/ultrasql-optimizer/src/lib.rs`,
  `crates/ultrasql-server/src/session/execute.rs`.
- Execution: `crates/ultrasql-server/src/pipeline/lower_query.rs`,
  `crates/ultrasql-server/src/pipeline/modify.rs`,
  `crates/ultrasql-executor/src/modify.rs`.
- Tests: `crates/ultrasql-parser/src/statements/merge.rs`,
  `crates/ultrasql-planner/src/binder/tests.rs`,
  `crates/ultrasql-server/tests/merge_round_trip.rs`.
