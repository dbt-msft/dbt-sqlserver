# Transaction scope and lock behaviour

This page describes how the SQL Server adapter scopes transactions around a
`table`, `incremental` or `snapshot` build, why the boundaries sit where they do, and
the one knob that moves them — the `pre_hook_transaction_scope` model config.

Background: [dbt-msft/dbt-sqlserver#819](https://github.com/dbt-msft/dbt-sqlserver/issues/819).

## The problem

SQL Server holds a statement's locks until the enclosing transaction commits,
not until the statement finishes. `Sch-M` (schema modification) is the one lock
mode incompatible with `Sch-S`, which *every* metadata reader takes — including
database-wide `sys` / `INFORMATION_SCHEMA` scans, a second dbt run's catalog
lookups, and an SSMS object explorer refresh. None of those asked for your
table by name.

So a `CREATE` that shares a transaction with the load that follows it blocks
every metadata reader in every other session for the whole load. Measured
against SQL Server 2022, with an open transaction in one session and a
`sys.tables` scan in another:

| Uncommitted statement | Catalog scan in another session |
|---|---|
| `SELECT * INTO` (fused create and load) | blocked |
| `SELECT TOP 0 * INTO` (empty create) | blocked |
| committed create, then `INSERT ... WITH (TABLOCK)` | **not blocked** |
| `CREATE CLUSTERED COLUMNSTORE INDEX` | blocked |
| `DROP VIEW` | blocked |
| `sp_rename` | blocked |
| `CREATE NONCLUSTERED INDEX` | blocked |

The load itself is harmless inside a transaction. Everything DDL-shaped that
shares a transaction with it is the problem.

### Before

```
BEGIN  ← first in-tx pre-hook statement (or the build itself)
│
├─ in-tx pre-hooks
├─ CREATE VIEW model__dbt_tmp_vw
├─ SELECT * INTO model__dbt_tmp                ← Sch-M on intermediate, held from here
├─ CREATE CLUSTERED COLUMNSTORE INDEX          ← Sch-M, also long
├─ sp_rename target → backup                   ← Sch-M on the live name
├─ sp_rename intermediate → target
├─ apply_masks                                 ← ALTER on the live target
├─ create_indexes                              ← Sch-M on the live target, long
├─ in-tx post-hooks
├─ grants / denies / persist_docs
COMMIT                                         ← every lock above released here
```

Two separate windows, both spanning slow work: the intermediate's `Sch-M`
across the load and the columnstore build, and the live target's `Sch-M` across
the index builds.

### After

```
   ├─ outside-tx pre-hooks                       autocommit
   ├─ CREATE OR ALTER VIEW model__dbt_tmp_vw     autocommit
   ├─ SELECT TOP 0 * INTO model__dbt_tmp         autocommit; Sch-M for an instant
   │
BEGIN  ← first in-tx pre-hook statement, if any
├─ in-tx pre-hooks
├─ INSERT INTO model__dbt_tmp WITH (TABLOCK)   ← X table lock, never Sch-M
├─ CREATE CLUSTERED COLUMNSTORE INDEX          ← on a private name (see residual window)
├─ sp_rename target → backup                   ← Sch-M on the live name
├─ sp_rename intermediate → target
├─ apply_masks                                 ← inside, deliberately (see below)
├─ in-tx post-hooks
COMMIT                                         ← the cutover is atomic; Sch-M released
   │
   ├─ DROP VIEW model__dbt_tmp_vw
   ├─ create_indexes                           ← outside; no Sch-M on a live name held to commit
   ├─ grants / denies / persist_docs
   ├─ drop backup, outside-tx post-hooks
```

With no in-transaction pre-hook, `BEGIN` happens at the first `sp_rename`
instead: the load and the columnstore build then autocommit too, and no
`Sch-M` is held for longer than a statement anywhere before the cutover.

`INSERT ... WITH (TABLOCK)` takes an exclusive *table* lock, which is compatible
with `Sch-S`, so the long load never blocks a metadata reader. The hint is what
keeps the load minimally logged — do not remove it to "reduce blocking".

The same shape applies to `incremental` on its first build, on `--full-refresh`,
and on the append/merge path, where the `__dbt_temp` build is staged ahead of
the hooks the same way; and to `snapshot`, whose first build goes through the
intermediate and is renamed into place, and whose staging table on later runs
is staged ahead of the hooks and merged inside the transaction.
`table_refresh_method: dml` stages its scratch table ahead of the hooks and
swaps with `DELETE` + `INSERT` inside the transaction.

## What is atomic with what

The transaction spans **in-transaction pre-hooks → the load → the cutover →
in-transaction post-hooks**. That is what a hook declaring `transaction: true`
is asking for: atomicity with *the model*. A `transaction: true` pre-hook's
writes roll back with a failed load, exactly as before. Index reconciliation,
grants, denies and `persist_docs` are the adapter's own housekeeping and were
never part of that promise, so they now run outside it.

**Masks are the exception.** On a path that builds a brand-new table (the
default rename swap, `full_refresh_build: prebuilt`, an incremental first build
or `--full-refresh`, a snapshot's first build, and the DML fallback), the table carries no masks until
`apply_masks` runs. If that ran after the cutover committed, a mask failure
would leave the newly loaded table live with its columns exposed. Masks
therefore stay inside the transaction on those paths. The ALTERs are cheap next
to an index build.

On the rename swap and the DML fallback a mask failure rolls the swap back, so
the old, masked table keeps serving. `full_refresh_build: prebuilt` has no swap
to roll back — it drops the target and rebuilds in place — so a mask failure
there leaves a loaded target carrying the `dbt_full_refresh_incomplete` marker,
which blocks normal runs until a `--full-refresh` succeeds. That is the
trade-off `prebuilt` already makes.

On the persisted-table paths (`table_refresh_method: dml` swap, incremental
append, snapshot merge) the table already carries its masks, so reconciliation runs outside — a
failure there leaves the previous masks in place, exposing nothing.

## Post-hook ordering changed

In-transaction post-hooks now run **before** index creation, where they
previously ran after, on `table`, `incremental` and `snapshot`. Masks on fresh tables
are unaffected — they still run before the post-hooks, for the reason in the
previous section. Post-hooks already ran before grants, denies and
`persist_docs` on `table`; on `incremental` and `snapshot` those three moved
after the post-hooks too, so the three materializations now share one tail.

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
  post-hook on a persisted-table path is now dropped by the same run's
  reconciliation. Move it to the `indexes` config.
- An index created by an in-transaction post-hook on a column in your `masks`
  config will trip the index-key check on SQL Server versions before 2022,
  where previously the ordering happened to avoid it.

## `pre_hook_transaction_scope`

Schema resolution — the tmp view, and under an enforced contract the describe
probe — needs the model SQL to *bind*: every object it references must exist.
Running it before the in-transaction pre-hooks is what lets it autocommit, so
that is also the one thing it cannot do: bind against an object a
`transaction: true` pre-hook is about to create.

| Value | Schema resolution runs | Transaction covers | Pre-hook rolls back with a failed load | #819 fixed |
|---|---|---|---|---|
| `load` (default) | before the in-tx pre-hooks, autocommitted | pre-hooks + load + cutover + post-hooks | yes | yes |
| `build` | inside the pre-hooks' transaction | pre-hooks + create + load + cutover + post-hooks | yes | no |

Under `load`, a `transaction: true` pre-hook that creates what the model reads
fails at the stage with `Invalid object name`, before any hook has run. Two
remedies:

- Declare that hook `transaction: false`. Outside-transaction pre-hooks run
  before the stage, so the object exists when the view binds. A staging-table
  refresh is rarely something you want rolled back anyway.
- Set `build` on the model. The create then runs after the hooks, inside their
  transaction, and holds the new table's `Sch-M` for the whole load — today's
  behaviour, for that model only.

```yaml
models:
  my_project:
    staging:
      +pre_hook_transaction_scope: build
```
```jinja
{{ config(pre_hook_transaction_scope='build') }}
```

Note that a view already cannot reference a `#temp` table, so a pre-hook that
stages what the model reads only ever worked with a permanent object; hooks
that truncate, disable indexes, write audit rows or refresh a source the model
already references bind fine under `load`.

**Where the setting is inert.** `full_refresh_build: prebuilt` builds in place:
its setup has to run after the hooks (a hook may read `{{ this }}` before the
rebuild drops it), and it commits its in-progress marker — and with it any
`transaction: true` pre-hook — before the load, precisely so the marker
survives a failure. An incremental `--full-refresh` of an existing table marks
it in progress the same way. On both, the pre-hook is durable by the time the
load runs whatever this is set to; use `transaction: false` and handle the
rollback yourself if that matters. The load itself is autocommitted on both,
so neither holds `Sch-M` across it.

**With no transactional pre-hook** the setting changes nothing: the stage
autocommits either way, and the load autocommits too. Note that
`transaction: true` is dbt's *default* for a pre-hook, so a plain string
pre-hook is a transactional one.

## Residual window

With a `transaction: true` pre-hook and `as_columnstore: true` (the default),
the clustered columnstore index is built on the intermediate inside the hook's
transaction, so its `Sch-M` — on a private name, but visible to database-wide
catalog scans — is held from the end of the columnstore build to the cutover
commit. That is bounded by the index build and the cutover, not the load. With
no transactional pre-hook the columnstore build autocommits and there is no
window. Closing it entirely would mean creating the columnstore index on the
empty table and bulk-loading into it, which is the `prebuilt` trade.

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
- A failed load under `load` leaves the empty `__dbt_tmp` intermediate and its
  tmp view behind, since both committed before the hook opened the transaction.
  The next run drops them before it starts.
