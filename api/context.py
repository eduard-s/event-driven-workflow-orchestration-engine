# API contexts share bounded validation processes for each database lifetime.

import asyncio
from dataclasses import dataclass, field
from weakref import WeakKeyDictionary

from api.config import SLOT_WAIT_SECONDS, VALIDATION_CONCURRENCY, VALIDATION_WORKERS, Settings
from shared.context import Context as ServiceContext
from shared.cpu import BoundedProcessPool
from shared.database import Database
from shared.errors import DomainError

_pools: WeakKeyDictionary[Database, BoundedProcessPool] = WeakKeyDictionary()


@dataclass(frozen=True)
class Context(ServiceContext[Settings]):
    # Share API dependencies and one validation pool for each database lifetime.
    #
    # Args:
    #     db: The database wrapper that owns validation-pool cleanup.
    #     settings: API settings for the operations that use this context.
    #
    # Returns:
    #     An immutable context with a shared validation pool.
    #
    # Attributes:
    #     validation: The bounded process pool assigned by __post_init__.

    validation: BoundedProcessPool = field(init=False)

    def __post_init__(self) -> None:
        # Reuse a validation pool so contexts cannot multiply CPU capacity.
        #
        # No arguments beyond the context instance.
        #
        # Returns:
        #     None. The function assigns validation and registers cleanup on the database.
        pool = _pools.get(self.db)
        if pool is None:
            pool = BoundedProcessPool(
                workers=VALIDATION_WORKERS,
                capacity=VALIDATION_CONCURRENCY,
                wait_seconds=SLOT_WAIT_SECONDS,
            )
            _pools[self.db] = pool
            self.db.add_cleanup(pool.close)
        object.__setattr__(self, "validation", pool)


async def acquire_slot(semaphore: asyncio.Semaphore) -> None:
    # Wait briefly for capacity so overload cannot create an unlimited queue.
    #
    # Args:
    #     semaphore: The admission semaphore whose slot the caller will release.
    #
    # Returns:
    #     None after the caller owns one slot.
    #
    # Raises:
    #     DomainError: No slot becomes available within SLOT_WAIT_SECONDS.
    try:
        async with asyncio.timeout(SLOT_WAIT_SECONDS):
            await semaphore.acquire()
    except TimeoutError as exc:
        raise DomainError(503, "OVERLOADED", "Request capacity is full.") from exc
