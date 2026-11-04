"""Regression tests for tiny-mq (added 2026-11-04).

The library shipped 1.0.0 with a module-level ``NameError``: the annotation
``Callable[[Message], Coroutine]`` referenced a name that was never imported,
so ``import tiny_mq`` raised before a single line of the library could run.
There was no test suite and no CI, so nothing noticed.
"""
import asyncio
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))


def test_module_imports():
    """The module must import. This is the test that would have caught it."""
    import tiny_mq

    assert tiny_mq.__version__ == "1.0.0"


def test_public_api_present():
    import tiny_mq

    for name in ("MessageQueue", "AsyncMessageQueue", "Message", "Priority"):
        assert hasattr(tiny_mq, name), f"missing public name: {name}"


def test_publish_and_subscribe_roundtrip():
    from tiny_mq import MessageQueue

    q = MessageQueue()
    seen = []
    q.subscribe("orders", lambda m: seen.append(m.body))
    q.publish("orders", {"id": 1})
    assert seen == [{"id": 1}]


def test_async_queue_imports_and_publishes():
    from tiny_mq import AsyncMessageQueue

    async def run():
        q = AsyncMessageQueue()
        await q.publish_async("events", "hello")
        return q

    q = asyncio.run(run())
    assert q is not None
