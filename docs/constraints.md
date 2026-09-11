# Constraints: behaviour specification

How `dbt-sqlserver` turns the constraints declared in a model's yaml into
database objects. Each rule below has an identifier; the table at the end maps
every rule to the test that verifies it. The user-facing summary is the
[Constraints section of the README](../README.md#constraints); this document is
the contract the code and the tests are held to. Issue:
[#579](https://github.com/dbt-msft/dbt-sqlserver/issues/579).

Terms: a **column-level** constraint is declared under a column's
`constraints:`; a **model-level** one under the model's own `constraints:` key
and names its `columns:`. A constraint is **named** when it carries `name:`.
The **build** is whatever creates the model's table on a given run; the
**swap** is the rename of `<model>__dbt_tmp` into place and of the outgoing
table to `<model>__dbt_backup`, which is then dropped.

## 1. When constraints apply

- **C1.1** Constraints are emitted only when the model's contract is enforced
  (`contract: {enforced: true}`). With the contract off, declared constraints
  are ignored without error, as on every dbt adapter.
- **C1.2** `not_null`, `check`, `unique`, `primary_key`, `foreign_key` and
  `custom` are rendered. dbt-core's own `warn_unsupported` / `warn_unenforced`
  handling applies unchanged.
- **C1.3** Constraints apply on every build path a contract-enforced model can
  take: `table` (rename-swap, `full_refresh_build: prebuilt`,
  `table_refresh_method: dml` in both its steady state and its schema-change
  fallback) and `incremental` (first build, steady state, `--full-refresh`).
  Snapshots and views carry no constraints.

## 2. Where a constraint is rendered

- **C2.1** A column-level constraint is rendered inline, in the `CREATE TABLE`
  column list, immediately after its column.
- **C2.2** A `check` declared at column level is hoisted out of the column
  definition into a table-level clause of the same `CREATE TABLE`. SQL Server
  accepts one column-level `CHECK` per column and any number at table level;
  hoisting makes several checks on one column legal. The result is anonymous
  either way.
- **C2.3** An **unnamed** model-level constraint is rendered inline, as a
  table-level clause of the `CREATE TABLE`, after the columns.
- **C2.4** A **named** model-level constraint is not rendered into the
  `CREATE TABLE`. It is applied by `ALTER TABLE <target> ADD CONSTRAINT <name>
  ...` after the build has committed, swapped the new table in, and dropped the
  outgoing one. Rationale: SQL Server scopes constraint names per schema, not
  per table, and the new table is built while the old one still exists, so an
  inline name would collide (`Msg 2714`) on every rebuild after the first.
- **C2.5** All named constraints of one model are applied in one batch, one
  round trip, outside dbt's model transaction (`auto_begin=False`), because the
  materialization has already committed.
- **C2.6** Each `ADD CONSTRAINT` in that batch is guarded by an existence test
  on `sys.objects` (`name` + `parent_object_id`), so the batch is a no-op for
  a constraint the table already holds. That makes the batch safe on build
  paths that keep the existing table (steady-state `incremental`, `dml`).
- **C2.7** A `name:` on a **column-level** constraint is ignored, with a
  warning that points at the model-level form. The constraint itself is still
  rendered, anonymously (C2.1).

## 3. What is rendered

- **C3.1** `primary_key` and `unique` render as `primary key nonclustered` /
  `unique nonclustered` by default, so they coexist with the clustered
  columnstore index built for `as_columnstore` (the adapter default).
- **C3.2** dbt's `expression` field on a `primary_key` or `unique` selects the
  clustering: `clustered` or `nonclustered`, matched case-insensitively and
  with surrounding whitespace ignored. It is emitted between the keyword and
  the column list, which is the only position T-SQL has there.
- **C3.3** Any other `expression` value on a `primary_key` or `unique` raises
  `DbtValidationError` (*Invalid expression ...*) at render time. For a named
  constraint that validation happens when the `CREATE TABLE` is rendered, not
  when its `ALTER` runs, so the run fails before anything is built.
- **C3.4** `expression: clustered` on a model with `as_columnstore` on fails at
  the database (*Cannot create more than one clustered index*), because the
  columnstore index already holds the clustered slot. The adapter does not
  pre-empt this; the README says to set `as_columnstore: false`.
- **C3.5** `foreign_key` accepts `to:` with `to_columns:` (the form dbt-core
  produces from `ref()`) and the older free-text `expression:` form
  (`<table> (<columns>)`). `to:` is passed through as rendered by dbt-core,
  database qualifier included: SQL Server accepts a three-part `REFERENCES`
  target that names the current database, and a genuinely cross-database
  target fails with SQL Server's own *references invalid table* error rather
  than being rewritten.
- **C3.6** A `foreign_key` with neither form is dropped with a warning; nothing
  is rendered for it.
- **C3.7** Column names in a model-level constraint's column list, and the
  referenced columns of a foreign key, are quoted with the adapter's
  identifier quoting. A constraint's name is quoted in the `ALTER` and passed
  as an escaped string literal in the existence guard.
- **C3.8** `not_null`, `check` and `custom` at column level are rendered by
  dbt-adapters' base renderer, unchanged. A `check` without an expression
  renders nothing.

## 4. Failure modes

- **C4.1** A violation of an **unnamed** constraint surfaces while the new
  table is loaded, before the swap. The run fails; the previous table, its
  rows and its constraints are untouched.
- **C4.2** A violation of a **named** constraint surfaces in the post-commit
  `ALTER`. The run fails; the new table is already in place and carries every
  constraint except the one that failed. `post_hook`s with
  `transaction: false` do not run. The next successful build re-applies it.
- **C4.3** A failed load may leave a `<model>__dbt_tmp` behind, depending on
  where the transaction boundary falls on that build path. The next successful
  build of the model clears it; a finished successful run leaves no
  `__dbt_tmp` or `__dbt_backup` object.
- **C4.4** A `foreign_key` that points at another model makes the **parent**'s
  rebuild fail at the backup drop (`Msg 3726`). The swap itself has already
  happened: the new parent is in place *without its own named constraints*
  (they are applied after the drop that failed), and the child's key now
  references `<parent>__dbt_backup`, because a foreign key follows the object,
  not the name. A plain `dbt run` does not recover: the parent trips over the
  same backup and the child is skipped. Rebuilding the child alone fails too
  (no parent key to reference) but its swap drops the stale key, after which
  a plain run rebuilds both; dropping the child's key by hand does the same.
  Prevention is the shipped `drop_fk_constraints()`
  macro as a `pre_hook` on the parent; its cost is that the child's key does
  not exist between the parent's rebuild and the child's next build.
  `table_refresh_method: dml` is not a workaround (its `DELETE` fails with
  `Msg 547` once the child holds rows, and its schema-change path hits
  `Msg 3726` anyway).

## 5. Changing a constraint on a table that persists

These follow from C2.4 and C2.6 and hold for **named** constraints on
`incremental` (steady state) and `table_refresh_method: dml` models.

- **C5.1** Adding a named constraint to an existing model lands on its next
  run, without `--full-refresh`.
- **C5.2** Removing one from the yaml does not drop it from the database.
- **C5.3** Renaming a `check`, `unique` or `foreign_key` adds the new name
  beside the old one. Renaming a `primary_key` fails with `Msg 1779` and the
  old key stays.
- **C5.4** Changing a constraint's definition under an unchanged name is not
  detected. The existence guard tests the name only.
- **C5.5** `--full-refresh` rebuilds the table, after which exactly the
  constraints the yaml declares exist.
- **C5.6** An **unnamed** constraint follows the table: on a persisting table,
  adding or changing one does nothing until the table is rebuilt, and the run
  still succeeds.

## 6. Interactions

- **C6.1** Masks. An unnamed `primary_key` / `unique` on a masked column is
  rejected by `apply_masks` (*is also an index key column*): its index exists
  before the masks are applied. A named one is applied after the masks and is
  allowed.
- **C6.2** Bulk load. A contract-enforced model is always loaded with `CREATE
  TABLE` + `INSERT ... WITH (TABLOCK)`, on every build path. A `primary_key` or
  `unique` (named or not) puts a nonclustered index on the table before that
  load, which is then maintained per row and fully logged. `check`, `not_null`
  and `foreign_key` create no index.
- **C6.3** dbt unit tests. The fixture table dbt builds for the model under
  test never carries the contract's keys, so fixture rows may violate a
  `primary_key`, `unique` or `foreign_key` without failing the test. (dbt's
  unit materialization builds that table with `temporary=True`, which takes
  the uncontracted path; `sqlserver__unit_test_create_table_as` additionally
  renders only `not_null` if it is ever dispatched, but nothing calls it
  today.)
- **C6.4** `XACT_ABORT`. A constraint violation inside the load batch aborts
  the whole batch and rolls back the open transaction, so no partial result is
  committed.

## Traceability

| Rule | Verified by |
|---|---|
| C1.1 | `test_constraints_e2e.py::TestTableModelLifecycle` step 1 |
| C1.2, C3.8 | `tests/unit/.../test_constraints.py::TestRenderColumnConstraint` |
| C1.3 | `TestTableModelLifecycle`, `TestIncrementalConstraintChanges`, `TestPrebuiltBuildCarriesConstraints`, `test_constraints_applied.py::TestDmlRefreshKeepsConstraints` |
| C2.1, C2.3 | `test_constraints_applied.py::TestAnonymousConstraints`; `tests/functional/adapter/dbt/test_constraints.py` (generated SQL) |
| C2.2 | `TestRenderRawColumnsConstraints`; `test_constraints_e2e.py::TestSeveralChecksOnOneColumn` |
| C2.4 | `test_constraints_applied.py::TestNamedModelConstraints::test_rebuild_reuses_the_same_constraint_names`; `TestTableModelLifecycle` step 3 |
| C2.5, C2.6 | `TestRenderRawModelAlterConstraints`; `test_constraints_applied.py::TestIncrementalConstraints`, `TestConstraintAddedToAnExistingModel` |
| C2.7 | `test_constraints_applied.py::TestNamedColumnConstraintWarns`; unit `test_a_name_is_ignored_but_the_constraint_is_still_rendered` |
| C3.1, C3.2 | unit `test_expression_chooses_the_clustering`; `test_constraints_applied.py::TestClusteredOverride`; `test_constraints_e2e.py::TestClusteringExpression` steps 2, 4 |
| C3.3 | unit `test_anything_but_the_two_keywords_is_rejected`, `test_a_named_constraint_still_validates_its_expression_up_front`; `TestClusteringExpression` step 1 |
| C3.4 | `TestClusteringExpression` step 3 |
| C3.5 | unit `test_foreign_key_from_to_and_to_columns`, `test_foreign_key_from_expression`; `test_constraints_applied.py::TestForeignKeyToRef` |
| C3.6 | unit `test_foreign_key_without_a_target_renders_nothing` |
| C3.7 | `TestRenderRawModelAlterConstraints::test_only_named_constraints_are_altered_in`, `test_the_bare_name_is_returned_for_the_existence_guard` |
| C4.1 | `TestTableModelLifecycle` step 4; `TestSeveralChecksOnOneColumn` step 2 |
| C4.2 | `TestTableModelLifecycle` step 6 |
| C4.3 | `TestTableModelLifecycle` steps 5, 7 |
| C4.4 | `test_constraints_e2e.py::TestForeignKeyParentRebuild` |
| C5.1 | `TestIncrementalConstraintChanges` step 3; `TestConstraintAddedToAnExistingModel` |
| C5.2 | `TestIncrementalConstraintChanges` step 4 |
| C5.3 | `TestIncrementalConstraintChanges` steps 5, 6 |
| C5.4 | not testable by observation; documented |
| C5.5 | `TestIncrementalConstraintChanges` step 7 |
| C5.6 | documented; `TestAnonymousConstraints::test_rebuild_does_not_collide` covers the rebuild half |
| C6.1 | `test_constraints_applied.py::TestNamedConstraintOnAMaskedColumn` |
| C6.2 | documented (README, Build-shape notes) |
| C6.3 | `test_constraints_e2e.py::TestUnitTestFixturesIgnoreKeys`; unit `test_not_null_only_drops_the_rest` |
| C6.4 | `test_xact_abort.py` (pre-existing) |
