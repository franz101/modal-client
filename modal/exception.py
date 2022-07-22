class RemoteError(Exception):
    pass


class AuthError(Exception):
    pass


class ConnectionError(Exception):
    pass


class InvalidError(Exception):
    """Used when user does something invalid."""


class VersionError(Exception):
    pass


class NotFoundError(Exception):
    pass


class ExecutionError(Exception):
    """Something unexpected happen during runtime."""
