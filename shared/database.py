from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from shared.config import DatabaseSettings as Settings

type Connection = AsyncConnection[dict[str, Any]]


class Database:
    # Own pooled PostgreSQL connections and dependent resources for one service lifetime.
    #
    # Attributes:
    #     pool: Connection pool that supplies dictionary rows to transactions.
    #
    # Returns:
    #     A database wrapper. Its constructor does not open connections.
    def __init__(self, settings: Settings) -> None:
        # Create a bounded connection pool without network I/O so startup controls when connections open.
        #
        # Args:
        #     settings: PostgreSQL connection string and maximum pool size.
        #
        # Returns:
        #     None. The connection pool and cleanup stack are ready to open.
        self._cleanup = AsyncExitStack()
        self.pool = AsyncConnectionPool[Connection](
            settings.postgres_dsn,
            min_size=1,
            max_size=settings.pool_size,
            timeout=3,
            open=False,
            kwargs={"autocommit": True, "row_factory": dict_row, "connect_timeout": 3},
        )

    async def open(self) -> None:
        # Open the pool before service work so connection failures stop startup.
        #
        # No arguments.
        #
        # Returns:
        #     None when the pool has its initial connection.
        #
        # Raises:
        #     Exception: The pool cannot open within the 15-second startup limit.
        await self.pool.open(wait=True, timeout=15)

    async def close(self) -> None:
        # Close registered resources and the pool so service shutdown releases processes and connections.
        #
        # No arguments.
        #
        # Returns:
        #     None after cleanup. The pool closes even when a resource callback fails.
        #
        # Raises:
        #     Exception: A resource callback or pool cleanup fails.
        try:
            await self._cleanup.aclose()
        finally:
            await self.pool.close()

    def add_cleanup(self, close: Callable[[], Awaitable[None]]) -> None:
        # Register dependent cleanup so CPU pools share the database owner's lifetime.
        #
        # Args:
        #     close: No-argument asynchronous callback. Callbacks run in reverse registration order.
        #
        # Returns:
        #     None. The callback remains registered until database cleanup.
        self._cleanup.push_async_callback(close)

    @asynccontextmanager
    async def tx(self, snapshot: bool = False) -> AsyncGenerator[Connection]:
        # Supply one transaction with bounded lock and statement waits so database work cannot wait indefinitely.
        #
        # Args:
        #     snapshot: True selects a repeatable-read, read-only transaction for consistent responses.
        #
        # Yields:
        #     A pooled connection. Normal exit commits, and an exception rolls back.
        #
        # Raises:
        #     Exception: Pool admission, SQL, commit, or rollback fails.
        async with self.pool.connection() as conn, conn.transaction():
            if snapshot:
                await conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            await conn.execute("SET LOCAL lock_timeout = '2s'")
            await conn.execute("SET LOCAL statement_timeout = '5s'")
            yield conn

    async def healthy(self) -> bool:
        # Read the workflow table without rows so readiness detects connection or schema failures.
        #
        # No arguments.
        #
        # Returns:
        #     True when the expected empty-table count succeeds. Otherwise False.
        #
        # Raises:
        #     Exception: Connection admission or the schema probe fails.
        async with self.tx() as conn:
            row = await (
                await conn.execute("SELECT count(*) AS count FROM executions WHERE false")
            ).fetchall()
            return row == [{"count": 0}]
