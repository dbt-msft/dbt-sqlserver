"""
Load-aware admission control for SQL Server connections (The Sentinel).

Queries sys.dm_os_schedulers to measure CPU pressure before allowing a new
query through.  When the number of runnable tasks exceeds a configurable
threshold, new queries are held back with an exponential-free, fixed-interval
back-off loop.  This prevents dbt's high-thread concurrency from saturating
the SQL Server scheduler and causing cascading timeouts.

The health-check query uses WITH (NOLOCK) and targets a lightweight DMV,
keeping execution time well under 10 ms even on a busy server.

Safety: the back-off loop is capped at ``DEFAULT_MAX_RETRIES`` iterations.
If the server is still busy after exhausting retries, the query proceeds
anyway to avoid a total deadlock of the dbt run.
"""

import time
from typing import Any

from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("sqlserver")

# NOLOCK prevents the health check itself from being blocked by schema locks.
# sys.dm_os_schedulers is an in-memory DMV — no I/O, no blocking, <10 ms.
SCHEDULER_HEALTH_QUERY = """
SELECT SUM(runnable_tasks_count)
FROM sys.dm_os_schedulers WITH (NOLOCK)
WHERE status = 'VISIBLE ONLINE'
"""

# Default thresholds — tuned for typical dbt workloads.
# runnable_tasks_count > 4 usually indicates the server is under meaningful
# CPU pressure and additional concurrent queries would degrade throughput.
DEFAULT_RUNNABLE_THRESHOLD = 4
DEFAULT_MAX_RETRIES = 10
DEFAULT_BACKOFF_SECONDS = 1.0


def get_scheduler_health(
    connection_handle: Any,
    threshold: int = DEFAULT_RUNNABLE_THRESHOLD,
) -> bool:
    """
    Check if SQL Server is under acceptable load.

    Returns True if healthy (runnable tasks <= threshold), False if busy.
    Uses a dedicated cursor so it never interferes with an in-flight query.
    """
    cursor = connection_handle.cursor()
    try:
        cursor.execute(SCHEDULER_HEALTH_QUERY)
        row = cursor.fetchone()
        runnable = row[0] if row and row[0] is not None else 0
        logger.debug(f"Scheduler health: {runnable} runnable tasks (threshold={threshold})")
        return runnable <= threshold
    finally:
        cursor.close()


def wait_for_healthy_server(
    connection_handle: Any,
    threshold: int = DEFAULT_RUNNABLE_THRESHOLD,
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
) -> bool:
    """
    Back-off loop that gates query execution until the server is healthy.

    Returns True if the server became healthy within the retry window,
    False if retries were exhausted (the caller should proceed anyway
    to avoid a total deadlock).
    """
    for attempt in range(1, max_retries + 1):
        if get_scheduler_health(connection_handle, threshold):
            return True

        logger.debug(
            f"Server busy, backing off {backoff_seconds}s "
            f"(attempt {attempt}/{max_retries})"
        )
        time.sleep(backoff_seconds)

    logger.warning(
        f"Server still busy after {max_retries} retries, proceeding to avoid deadlock."
    )
    return False
