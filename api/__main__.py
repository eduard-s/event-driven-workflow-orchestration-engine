import logging

import uvicorn

from api.app import create_app
from api.config import HTTP_HOST, HTTP_PORT, HTTP_SHUTDOWN_SECONDS, Settings
from shared.config import configure_logging


def main() -> None:
    # Start the HTTP service and its queued logs.
    #
    # No arguments. Environment settings select the limits and connections.
    #
    # Returns:
    #     None after Uvicorn stops. Startup errors propagate to the caller.
    settings = Settings()
    log_output = configure_logging(settings)
    try:
        for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "uvicorn.asgi"):
            logger = logging.getLogger(name)
            logger.handlers.clear()
            logger.propagate = True
        uvicorn.run(
            create_app(settings),
            host=HTTP_HOST,
            port=HTTP_PORT,
            log_level=settings.log_level.lower(),
            log_config=None,
            timeout_graceful_shutdown=HTTP_SHUTDOWN_SECONDS,
        )
    finally:
        log_output.close()


if __name__ == "__main__":
    main()
