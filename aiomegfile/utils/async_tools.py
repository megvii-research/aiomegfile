import inspect
import typing as T

__all__ = [
    "maybe_await",
]


async def maybe_await(value: T.Any) -> T.Any:
    """Await the given value when it is awaitable.

    :param value: Value which might be awaitable.
    :return: Awaited result for awaitables, otherwise original value.
    """
    if inspect.isawaitable(value):
        return await T.cast(T.Awaitable[T.Any], value)
    return value
