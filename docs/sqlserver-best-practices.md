# SQL Server Best Practices: Configuration and Usage

Several SQL Server defaults are wrong for a dbt workload — repeated bulk table
builds, run concurrently, read while they run. Each section gives the setting,
the value Microsoft recommends, and why it matters here.

- [Enable Read Committed Snapshot Isolation (RCSI)](#enable-read-committed-snapshot-isolation-rcsi)
- [Size and split tempdb](#size-and-split-tempdb)
- [Set max server memory and MAXDOP](#set-max-server-memory-and-maxdop)
- [Match the recovery model to the workload](#match-the-recovery-model-to-the-workload)
- [Use OPENQUERY instead of 4-part-name joins for linked servers](#use-openquery-instead-of-4-part-name-joins-for-linked-servers)
- [Worth validating on your own instance](#worth-validating-on-your-own-instance)

## Enable Read Committed Snapshot Isolation (RCSI)

Enable RCSI on any database read while dbt writes — BI tools, other pipelines,
dbt's own tests.

```sql
ALTER DATABASE <db> SET READ_COMMITTED_SNAPSHOT ON;
```

**Why it matters.** Under plain `READ COMMITTED`, readers take shared locks and
block behind every writer: incremental and snapshot DML, seed loads, the swap in
a table refresh. RCSI switches `READ COMMITTED` to row versioning database-wide,
so readers get the last committed rows without blocking and without blocking the
writer back. It also reduces lock-inversion deadlocks between concurrent models.

The adapter's `table_refresh_method: dml` is built on it — "DML-only table
refresh for use under RCSI … RCSI ensures concurrent readers see old data until
COMMIT"
([`table_dml_refresh.sql`](../dbt/include/sqlserver/macros/materializations/models/table/table_dml_refresh.sql)).
That path is opt-in (`table_refresh_method` defaults to `rename`,
[`table.sql`](../dbt/include/sqlserver/macros/materializations/models/table/table.sql)),
so it's the sharpest case for RCSI, not the only one.

**Limits.** Writers still block writers. RCSI does not cover DDL: the default
rename-swap takes a schema-modification lock and briefly leaves the table name
unresolvable. If readers must never see that window, pair RCSI with
`table_refresh_method: dml` — noting that a run which changes a model's columns
falls back to rename-swap anyway.

**Operational notes.** The `ALTER` requires no active connections other than its
own (not single-user mode) and does not return until existing transactions
commit. Being database-wide, it needs no per-query
`SET TRANSACTION ISOLATION LEVEL`. Row versions accumulate in tempdb — size it
accordingly.

---

## Size and split tempdb

One data file per logical processor up to eight, all the same size and growth
increment, preallocated so they never autogrow in normal operation. Add files in
multiples of four if allocation contention persists. Equal sizing is load-bearing:
the proportional-fill algorithm favors whichever file has the most free space, so
unequal files defeat the split. Put tempdb on fast storage and enable instant
file initialization.

```sql
SELECT name, size * 8.0 / 1024 AS size_mb, growth, is_percent_growth
FROM tempdb.sys.database_files;
```

**Why it matters.** Every model build spills sorts, hashes and work tables into
tempdb, and `threads > 1` multiplies that concurrently — the allocation
contention multiple files exist to relieve. RCSI's version store also lives
there, so enabling RCSI without tempdb headroom trades a blocking problem for an
out-of-space one. Defaults start each file at 8 MB with 64 MB autogrowth, so an
untuned instance regrows files during every run.

```sql
SELECT SUM(version_store_reserved_page_count) * 8.0 / 1024 AS version_store_mb,
       SUM(internal_object_reserved_page_count) * 8.0 / 1024 AS internal_mb,
       SUM(unallocated_extent_page_count) * 8.0 / 1024 AS free_mb
FROM tempdb.sys.dm_db_file_space_usage;
```

---

## Set max server memory and MAXDOP

```sql
EXECUTE sp_configure 'show advanced options', 1;
RECONFIGURE;
EXECUTE sp_configure 'max server memory', <mb>;          -- default: 2,147,483,647
EXECUTE sp_configure 'max degree of parallelism', <n>;   -- default: 0
RECONFIGURE;
```

**max server memory** ships effectively unlimited, letting SQL Server starve the
OS. Microsoft's starting point is roughly 75% of system memory not consumed by
other processes; measure before settling on a number, and set it explicitly if
*Lock pages in memory* is granted.

**MAXDOP** ships at `0` — a single query may use every processor, which
Microsoft calls "not the recommended value for most cases":

| Server | Processors | MAXDOP |
| --- | --- | --- |
| Single NUMA node | ≤ 8 logical processors | at or under the processor count |
| Single NUMA node | > 8 logical processors | 8 |
| Multiple NUMA nodes | ≤ 16 logical processors per node | at or under the per-node count |
| Multiple NUMA nodes | > 16 logical processors per node | half the per-node count, max 16 |

**Why it matters.** dbt already parallelizes at the model level via `threads`.
With MAXDOP `0` each concurrently building model also fans out across every
core, so the two layers compete — `CXPACKET` waits and erratic run times as
thread count rises. Both settings take effect immediately, without a restart.

---

## Match the recovery model to the workload

Use `SIMPLE` unless the database holds data that exists nowhere else.

```sql
ALTER DATABASE <db> SET RECOVERY SIMPLE;
```

**Why it matters.** Under `FULL`, every row of every table build is fully logged
and the log grows until a log backup truncates it — a full-refresh run can
balloon the log or fail outright on disk-full. Under `SIMPLE` or `BULK_LOGGED`,
bulk operations qualify for minimal logging, recording page allocations rather
than row data. `SELECT INTO` is one of those operations and is how the adapter
materializes tables, so the recovery model governs how much log a `dbt run`
generates.

**Exceptions.** `SIMPLE` gives up point-in-time recovery: you restore only to the
last full or differential backup. Keep `FULL`, with real log backups, for landing
tables loaded straight from an API and for `snapshot` models, whose accumulated
history dbt cannot reconstruct.

---

## Use OPENQUERY instead of 4-part-name joins for linked servers

Never join *across* the local/remote boundary with a 4-part name. Land remote
data in a staging model via a pass-through `OPENQUERY`, then join local-to-local
downstream:

```sql
-- avoid: distributed join, re-executed by every model that needs it
select * from local_orders o
join LOCALLOOP.RemoteDb.dbo.remote_customers c on o.cust_id = c.id
```

```sql
-- models/staging/stg_remote_customer_orders.sql
{{ config(materialized='table') }}
select * from {{ sqlserver__openquery(
  'LOCALLOOP',
  "select c.id, c.name, o.order_date, o.total
   from RemoteDb.dbo.remote_customers c
   join RemoteDb.dbo.remote_orders o on o.cust_id = c.id
   where o.order_date >= '2026-01-01'"
) }}
```

The adapter ships
[`sqlserver__openquery`](../dbt/include/sqlserver/macros/utils/openquery.sql) for
this: it doubles single quotes in the remote SQL, strips carriage returns,
brackets the server name, and fails compilation with a specific message if the
escaped query exceeds the 8 KB limit or either argument is empty. Writing
`OPENQUERY(...)` by hand means escaping every literal yourself — the date
predicate above would need `''2026-01-01''`.

```sql
-- models/marts/orders_enriched.sql — a plain local join
select * from {{ ref('local_orders') }} o
join {{ ref('stg_remote_customer_orders') }} c on o.cust_id = c.id
```

Joins *inside* the `OPENQUERY` string are not distributed queries — the whole
string runs on the remote server as ordinary local SQL, using its indexes and
statistics, and carries no risk of error 3988. Filters belong in there too, like
the date predicate above: only matching rows travel.

**Why stage.** The remote table crosses the wire once per run instead of once per
consuming model, and downstream joins are local SQL the optimizer has statistics
for. The staging model is a real DAG node — `ref()`-able, testable, documented —
so a remote schema change fails one test instead of breaking every model that
inlined the remote name. Materialize it as `table` or `incremental`, not
`ephemeral`: ephemeral models are inlined into each consumer as a CTE, restoring
the per-model round trips staging removes. Inline `openquery(...)` only for a
one-off read nothing else consumes.

**Why not 4-part names.** The local optimizer splits the query into remote and
local parts and may pull rows back before filtering; Microsoft's guidance is to
"ensure that as much logic as possible is executed on the remote server."
Joining multiple remote tables this way also risks error **3988**, "New
transaction is not allowed because there are other threads running in the
session" — documented as a design limitation when a distributed query joins
multiple tables from one remote source, `XACT_ABORT` is ON, and the session is
not enlisted in a distributed transaction (workaround:
`BEGIN DISTRIBUTED TRANSACTION` / MSDTC). dbt-sqlserver sets `XACT_ABORT ON` on
every connection by default (`xact_abort`), so such joins land in exactly that
window.

**Caveats.** The `OPENQUERY` string maxes out at 8 KB — past that use
`EXEC(...) AT <server>` or a remote view, which is what the macro's error tells
you. `OPENQUERY` accepts no variables, so the remote SQL must be a literal
assembled at compile time — which is exactly what the Jinja macro does, and why
runtime values have to be rendered into the string rather than bound. Errors
3988 and 3998 are
distinct and widely conflated: 3998 is "Uncommittable transaction is detected at
the end of the batch. The transaction is rolled back."

---

## Worth validating on your own instance

Neither setting below has a published target value, and neither is measured
against a dbt workload here. Test before adopting.

**`cost threshold for parallelism`** (default `5`) decides which queries get a
parallel plan; MAXDOP caps how wide that plan goes. Microsoft on the default:
"a starting point, not a recommendation." Their symptoms tell you which way to
move — `CXPACKET`/`CXCONSUMER` dominating waits means too low,
`SOS_SCHEDULER_YIELD` dominating means too high. A dbt run is mostly large scans
and inserts, the queries you *want* parallel, so raising it may matter less here
than on an OLTP instance.

**`optimize for ad hoc workloads`** (off by default) caches a plan stub instead
of a full compiled plan on first compile, so single-use plans stop consuming
cache. dbt emits a lot of distinct one-shot SQL per run; whether that adds up
depends on project size:

```sql
SELECT objtype, cacheobjtype,
       SUM(refcounts) AS all_ref_objects,
       SUM(CAST(size_in_bytes AS BIGINT)) / 1024 / 1024 AS size_mb
FROM sys.dm_exec_cached_plans
WHERE objtype = 'Adhoc' AND usecounts = 1
GROUP BY objtype, cacheobjtype;
```

The cost is diagnostic: you can't view execution plans for single-use queries. It
affects new plans only, and Query Store (default-on in SQL Server 2022+) covers
much of the loss.

---

## Sources

- [ALTER DATABASE ... SET options - READ_COMMITTED_SNAPSHOT](https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-database-transact-sql-set-options?view=sql-server-ver17)
- [SET TRANSACTION ISOLATION LEVEL - READ COMMITTED + snapshot semantics](https://learn.microsoft.com/en-us/sql/t-sql/statements/set-transaction-isolation-level-transact-sql)
- [SQL Server transaction locking and row versioning guide](https://learn.microsoft.com/en-us/sql/relational-databases/sql-server-transaction-locking-and-row-versioning-guide)
- [tempdb database - file count, sizing, version store](https://learn.microsoft.com/en-us/sql/relational-databases/databases/tempdb-database?view=sql-server-ver17)
- [Recommendations to reduce allocation contention in tempdb](https://learn.microsoft.com/en-us/troubleshoot/sql/database-engine/performance/recommendations-reduce-allocation-contention)
- [Database instant file initialization](https://learn.microsoft.com/en-us/sql/relational-databases/databases/database-instant-file-initialization)
- [Server memory configuration options](https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/server-memory-server-configuration-options?view=sql-server-ver17)
- [Configure the max degree of parallelism option](https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/configure-the-max-degree-of-parallelism-server-configuration-option?view=sql-server-ver17)
- [Configure the cost threshold for parallelism option](https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/configure-the-cost-threshold-for-parallelism-server-configuration-option?view=sql-server-ver17)
- [optimize for ad hoc workloads](https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/optimize-for-ad-hoc-workloads-server-configuration-option?view=sql-server-ver17)
- [Monitoring performance by using the Query Store](https://learn.microsoft.com/en-us/sql/relational-databases/performance/monitoring-performance-by-using-the-query-store)
- [Recovery models](https://learn.microsoft.com/en-us/sql/relational-databases/backup-restore/recovery-models-sql-server?view=sql-server-ver17)
- [Prerequisites for minimal logging in bulk import](https://learn.microsoft.com/en-us/sql/relational-databases/import-export/prerequisites-for-minimal-logging-in-bulk-import?view=sql-server-ver17)
- [SELECT - INTO clause](https://learn.microsoft.com/en-us/sql/t-sql/queries/select-into-clause-transact-sql?view=sql-server-ver17)
- [OPENQUERY](https://learn.microsoft.com/en-us/sql/t-sql/functions/openquery-transact-sql?view=sql-server-ver17)
- [Query remote servers - OPENQUERY/OPENROWSET/EXEC AT](https://learn.microsoft.com/en-us/sql/relational-databases/linked-servers/linked-servers-openquery-openrowset-exec-at)
- [Error 3988](https://learn.microsoft.com/en-us/sql/relational-databases/errors-events/mssqlserver-3988-database-engine-error)
- [Errors 3000-3999 (error 3998)](https://learn.microsoft.com/en-us/sql/relational-databases/errors-events/database-engine-events-and-errors-3000-to-3999?view=sql-server-ver17)
- [KB 3049257 - error 3988 with distributed query joining multiple remote tables](https://support.microsoft.com/en-us/kb/3049257)
- [SET XACT_ABORT](https://learn.microsoft.com/en-us/sql/t-sql/statements/set-xact-abort-transact-sql)
