"""Tests for async utility helpers."""

from aiomegfile.utils.async_tools import maybe_await


async def test_maybe_await_with_plain_value() -> None:
    """maybe_await returns non-awaitable values directly."""
    value = await maybe_await(123)
    assert value == 123


async def test_maybe_await_with_coroutine() -> None:
    """maybe_await awaits coroutine values."""

    async def _coro() -> str:
        return "ok"

    value = await maybe_await(_coro())
    assert value == "ok"
