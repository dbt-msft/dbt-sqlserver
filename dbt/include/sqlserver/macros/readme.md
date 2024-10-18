# Alterations from Fabric

## `materialization incremental` 

This is reset to the original logic from the global project.

## `materialization view`

This is reset to the original logic from the global project

## `materialization table`

This is resets to the original logic from the global project

## `sqlserver__create_columns`

SQLServer supports ALTER; this updates the logic to apply alter instead of the drop/recreate

## `sqlserver__alter_column_type`

SQLServer supports ALTER; this updates the logic to apply alter instead of the drop/recreate


## `sqlserver__can_clone_table`

SQLServer cannot clone, so this just returns False

## `sqlserver__create_table_as`

Logic is slightly re-written from original.
There is an underlying issue with the structure in that its embedding in EXEC calls.

This creates an issue where temporary tables cannot be used, as they dont exist within the context of the EXEC call.

One work around might be to issue the create table from a `{{ run_query }}` statement in order to have it accessible outside the exec context.

Additionally the expected {% do adapter.drop_relation(tmp_relation) %} does not fire. Possible cache issue?
Resolved by calling `DROP VIEW IF EXISTS` on the relation

## `sqlserver__create_view_as`

Updated to remove `create_view_as_exec` call.

## `listagg`

DBT expects a limit function, but the sqlserver syntax does not support it. Fabric also does not implement this properly

## `sqlserver__snapshot_merge_sql`

Restores logic to the merge statement logic like the dbt core. Merge will probably be slower then the existing logic

## unit tests

To accomidate the nested CTE situation, we create a temp view for the actual/expected and use those both in the test.
