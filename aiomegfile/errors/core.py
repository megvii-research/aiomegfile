"""Core exceptions and retry helper utilities."""

from __future__ import annotations

import asyncio
import logging
import typing as T
from functools import wraps

from aiomegfile.config import DEFAULT_MAX_RETRY_TIMES
from aiomegfile.utils.path import PathLike

logger = logging.getLogger(__name__)

__all__ = [
    "aioretry",
    "ProtocolNotFoundError",
    "UnknownError",
    "full_class_name",
    "full_error_message",
]


def full_class_name(obj: object) -> str:
    """Return class name with module prefix when available.

    :param obj: Object instance.
    :return: Fully qualified class name.
    :rtype: str
    """
    module = obj.__class__.__module__
    if module is None or module == str.__class__.__module__:
        return obj.__class__.__name__
    return module + "." + obj.__class__.__name__


def full_error_message(error: Exception) -> str:
    """Return stable, readable error message with class name.

    :param error: Exception instance.
    :return: Rendered error text.
    :rtype: str
    """
    try:
        message = str(error)
    except Exception:
        message = repr(error)
    return "%s(%r)" % (full_class_name(error), message)


class ProtocolNotFoundError(Exception):
    """Raised when no filesystem implementation matches a protocol."""


class UnknownError(Exception):
    """Wrap an unknown protocol operation error with path context."""

    def __init__(self, error: Exception, path: PathLike, extra: str | None = None):
        """Initialize an ``UnknownError``.

        :param error: Original exception.
        :param path: Path involved in the failed operation.
        :param extra: Optional extra context.
        """
        message = "Unknown error encountered: %r, error: %s" % (
            path,
            full_error_message(error),
        )
        if extra is not None:
            message += ", " + extra
        super().__init__(message)
        self.path = path
        self.extra = extra
        self.__cause__ = error

    def __reduce__(self):
        """Support pickling while preserving wrapped cause and context.

        :return: State tuple used by pickle.
        :rtype: tuple[type[UnknownError], tuple[Exception, PathLike, str | None]]
        """
        return (self.__class__, (self.__cause__, self.path, self.extra))


def aioretry(
    should_retry: T.Callable[[Exception], bool],
    max_retries: int = DEFAULT_MAX_RETRY_TIMES,
    before_callback: T.Optional[T.Callable[..., T.Awaitable[None]]] = None,
    after_callback: T.Optional[T.Callable[..., T.Awaitable[T.Any]]] = None,
    retry_callback: T.Optional[T.Callable[..., T.Awaitable[None]]] = None,
):
    """Return async retry decorator with exponential backoff.

    :param should_retry: Predicate to decide whether to retry on an exception.
    :param max_retries: Maximum attempts including first execution.
    :param before_callback: Optional callback executed before the first attempt.
    :param after_callback: Optional callback executed after successful completion.
    :param retry_callback: Optional callback executed before each retry.
    :return: Decorator for async callables.
    """

    def decorator(func: T.Callable[..., T.Awaitable[T.Any]]):
        """Decorate async callable with retry logic.

        :param func: Async function to decorate.
        :return: Wrapped async function.
        """

        @wraps(func)
        async def wrapper(*args, **kwargs):
            """Execute wrapped function with retry behavior.

            :param args: Positional arguments for wrapped function.
            :param kwargs: Keyword arguments for wrapped function.
            :return: Wrapped function result.
            """
            if before_callback is not None:
                await before_callback(*args, **kwargs)

            for retries in range(1, max_retries + 1):
                try:
                    result = await func(*args, **kwargs)
                    if after_callback is not None:
                        result = await after_callback(result, *args, **kwargs)
                    if retries > 1:
                        logger.info(
                            "Error already fixed by retry %s times",
                            retries - 1,
                        )
                    return result
                except Exception as error:
                    if not should_retry(error):
                        raise
                    if retry_callback is not None:
                        await retry_callback(error, *args, **kwargs)
                    if retries >= max_retries:
                        logger.error(
                            "Cannot handle error %s after %s tries",
                            full_error_message(error),
                            retries,
                        )
                        raise
                    retry_interval = min(0.1 * 2**retries, 30)
                    logger.info(
                        "unknown error encountered: %s, retry in %.1fs after %s tries",
                        full_error_message(error),
                        retry_interval,
                        retries,
                    )
                    await asyncio.sleep(retry_interval)

        return wrapper

    return decorator
