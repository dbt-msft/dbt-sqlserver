# Transaction scope and lock behaviour

This page describes how the SQL Server adapter scopes transactions around a
model build, why the boundaries sit where they do, and the two knobs that move
them — the `pre_hook_transaction_scope` model config and the
`dbt_sqlserver_pre_hook_schema_scope` behaviour flag.

Background: [dbt-msft/dbt-sqlserver#819](https://github.com/dbt-msft/dbt-sqlserver/issues/819).

## The problem

SQL Server holds a statement's locks until the enclosing transaction commits,
not until the statement finishes. `Sch-M` (schema modification) is the one lock
mode incompatible with `Sch-S`, which *every* metadata reader takes — including
database-wide `sys` / `INFORMATION_SCHEMA` scans, a second dbt run's catalog
lookups, and an SSMS object explorer refresh. None of those asked for your
table by name.

So a `CREATE` that shares a transaction with the load that follows it blocks
every metadata reader in every other session for the whole load.

### Before

```
BEGIN  ← first in-tx pre-hook statement (or the build itself)
│
├─ in-tx pre-hooks
├─ CREATE VIEW model__dbt_tmp_vw
├─ SELECT TOP 0 * INTO model__dbt_tmp        ← Sch-M on intermediate, held from here
├─ INSERT INTO model__dbt_tmp WITH (TABLOCK) ← the long one
├─ CREATE CLUSTERED COLUMNSTORE INDEX        ← Sch-M, also long
├─ sp_rename target → backup                 ← Sch-M on the live name
├─ sp_rename intermediate → target
├─ apply_masks                               ← ALTER on the live target
├─ create_indexes                            ← Sch-M on the live target, long
├─ in-tx post-hooks
├─ grants / denies / persist_docs
COMMIT                                       ← every lock above released here
```

Two separate windows, both spanning slow work: the intermediate's `Sch-M`
across the load and the columnstore build, and the live target's `Sch-M` across
the index builds.

### After

```
BEGIN  ← first in-tx pre-hook statement
├─ in-tx pre-hooks
├─ CREATE VIEW model__dbt_tmp_vw
├─ SELECT TOP 0 * INTO model__dbt_tmp        ← Sch-M, but TOP 0 moves no rows
COMMIT                                       ← Sch-M released, effectively instant
   │
   ├─ INSERT INTO model__dbt_tmp WITH (TABLOCK)  ← autocommitted; X lock, never Sch-M
   ├─ CREATE CLUSTERED COLUMNSTORE INDEX         ← autocommitted, on a private name
   │
BEGIN
├─ sp_rename target → backup                 ← Sch-M on the live name
├─ sp_rename intermediate → target
├─ apply_masks                               ← inside, deliberately (see below)
├─ in-tx post-hooks
COMMIT                                       ← the cutover is atomic; Sch-M released
   │
   ├─ create_indexes                         ← outside; no Sch-M on a live name held to commit
   ├─ grants / denies / persist_docs
```

`INSERT ... WITH (TABLOCK)` takes an exclusive *table* lock, which is compatible
with `Sch-S`, so the long load never blocks a metadata reader. The hint is what
keeps the load minimally logged — do not remove it to "reduce blocking".

## What is atomic with what

The transaction spans **in-transaction pre-hooks → the cutover → in-transaction
post-hooks**. That is what a hook declaring `transaction: true` is asking for:
atomicity with *the model*. Index reconciliation, grants, denies and
`persist_docs` are the adapter's own housekeeping and were never part of that
promise, so they now run outside it.

**Masks are the exception.** On a path that builds a brand-new table (the
default rename swap, `full_refresh_build: prebuilt`, and the DML fallback), the
table carries no masks until `apply_masks` runs. If that ran after the cutover
committed, a mask failure would leave the newly loaded table live with its
columns exposed. Masks therefore stay inside the transaction on those paths, so
a failure rolls the swap back and the old, masked table keeps serving. The
ALTERs are cheap next to an index build.

On the `table_refresh_method: dml` swap path the table persists and already
carries its masks, so reconciliation runs outside — a failure there leaves the
previous masks in place, exposing nothing.

## Post-hook ordering changed

In-transaction post-hooks now run **before** masks and indexes, where they
previously ran after. They already ran before grants, denies and
`persist_docs`; those relationships are unchanged.

If a post-hook needs the indexes to exist — it queries the table at scale, or
creates an index of its own — declare it `transaction: false`. That slot runs
after the entire tail:

```yaml
post_hook:
  - sql: "update {{ this }} set ... "
    transaction: false
```

Two consequences worth knowing if you manage indexes through post-hooks (the
idiom that predates the `indexes` config):

- With `drop_unmanaged_indexes: true`, an index created by an in-transaction
  post-hook is now dropped by the same run's reconciliation. Move it to the
  `indexes` config.
- An index created by an in-transaction post-hook on a column in your `masks`
  config will trip the index-key check on SQL Server versions before 2022, where
  previously the ordering happened to avoid it.

## `pre_hook_transaction_scope`

A pre-hook's writes must be visible to the load, and SQL Server has one
transaction context per session with no autonomous transactions. So the load
either shares the pre-hook's transaction — holding `Sch-M` for its whole
duration — or the pre-hook is committed before it. There is no third option.

| Value | Transaction covers | Pre-hook rolls back with a failed load | #819 fixed |
|---|---|---|---|
| `schema` | pre-hooks + `CREATE VIEW` + the empty `CREATE` | no | yes |
| `build` | pre-hooks + the whole build | yes | no |

```yaml
models:
  my_project:
    +pre_hook_transaction_scope: build   # project or folder wide
```
```jinja
{{ config(pre_hook_transaction_scope='build') }}
```

Use `build` only when a pre-hook irreversibly *moves* state the model is the
sole consumer of — a destructive dequeue (`DELETE ... OUTPUT ... INTO`), or an
`ALTER TABLE ... SWITCH` partition-out. For the ordinary cases — disabling
indexes, audit rows, refreshing a staging table, grants — `schema` is correct
and cheaper.

**The setting only matters when a pre-hook actually left a transaction open.**
A model with no transactional pre-hook always takes the narrow path and always
gets the fix, whatever this is set to. Note also that `transaction: true` is
dbt's *default* for a pre-hook, so a plain string pre-hook is a transactional
one.

## `dbt_sqlserver_pre_hook_schema_scope`

Supplies the default for `pre_hook_transaction_scope`. It ships `False`
(meaning `build`) so current behaviour is preserved, and is expected to flip to
`True` in a later release.

```yaml
flags:
  dbt_sqlserver_pre_hook_schema_scope: True
```

While it is `False`, dbt prints a one-off behaviour-change notice per run. A
model that sets `pre_hook_transaction_scope` explicitly is never warned about —
the flag is only read when the config is unset.

## Caveats

- `apply_grants` opens a transaction of its own (dbt's `call_dcl_statements`
  uses the default `auto_begin`), so grants, denies and `persist_docs` still run
  inside one. They are short DCL statements and take no `Sch-M` on the target.
- An `online` or `resumable` index build runs outside any transaction by
  necessity, and the adapter closes the transaction that leaves open before
  continuing.
- Index and mask reconciliation running outside the cutover means a failure
  there leaves the new data committed with indexes not yet converged. Both
  reconcile against the config rather than applying a delta, so the next run
  converges them.
