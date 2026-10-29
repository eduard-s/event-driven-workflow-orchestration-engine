# Run the orchestrator independently of the HTTP server.

import asyncio
import logging
import signal
from contextlib import AsyncExitStack

from orchestrator.broker import Broker
from orchestrator.config import Settings
from orchestrator.runtime import Runtime
from shared.config import configure_logging
from shared.context import Context
from shared.database import Database

LOG = logging.getLogger(__name__)


async def main() -> None:
    # Run the independent orchestrator service and close its resources after stop or failure.
    #
    # No arguments.
    #
    # Returns:
    #     None after the stop signal and resource cleanup.
    #
    # Raises:
    #     RuntimeError: A control loop stops after an unrecovered failure.
    #     Exception: Configuration, startup, or cleanup fails.
    settings = Settings()
    log_output = configure_logging(settings)
    async with AsyncExitStack() as cleanup:
        cleanup.push_async_callback(asyncio.to_thread, log_output.close)
        database = Database(settings)
        cleanup.push_async_callback(database.close)
        broker = Broker(settings)
        cleanup.push_async_callback(broker.close)
        runtime = Runtime(Context(database, settings), broker)
        cleanup.push_async_callback(runtime.close)
        for sig in (signal.SIGTERM, signal.SIGINT):
            asyncio.get_running_loop().add_signal_handler(sig, runtime.stop.set)
        await database.open()
        await broker.start()
        await runtime.start()
        LOG.info("orchestrator_ready")
        await runtime.stop.wait()
        if runtime.failure:
            raise RuntimeError(f"Orchestrator loop failed: {runtime.failure}")


if __name__ == "__main__":
    asyncio.run(main())
