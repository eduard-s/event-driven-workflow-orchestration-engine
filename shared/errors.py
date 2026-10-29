class DomainError(Exception):
    # Carry a public failure code and status across service layers so the API can return a controlled error.
    #
    # Attributes:
    #     status: HTTP status for the failure.
    #     code: Stable public failure identifier.
    #     message: Public explanation for the failure.
    #     path: Optional source field or JSON path.
    #
    # Returns:
    #     An exception instance that carries these public fields.

    def __init__(self, status: int, code: str, message: str, path: str | None = None) -> None:
        # Store the public failure fields and exception text so callers can report a controlled error consistently.
        #
        # Args:
        #     status: HTTP status for the failure.
        #     code: Stable public failure identifier.
        #     message: Public explanation and exception text.
        #     path: Optional source field or JSON path. The default is None.
        #
        # Returns:
        #     None.

        self.status = status
        self.code = code
        self.message = message
        self.path = path
        super().__init__(message)
