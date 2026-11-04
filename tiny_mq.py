"""
tiny-mq — Zero-dependency in-process message queue with pub/sub.
Fast queues, topics, fan-out, and dead-letter handling — in one file.
"""
import asyncio
import json
import os
import time
import uuid
from collections.abc import Coroutine
from dataclasses import dataclass, field
from enum import IntEnum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

__version__ = "1.0.0"
__all__ = [
    "MessageQueue",
    "AsyncMessageQueue",
    "Message",
    "Priority",
    "MessageQueueError",
]


# ─── Enums & Exceptions ──────────────────────────────────────────────────────


class Priority(IntEnum):
    LOW = 0
    NORMAL = 5
    HIGH = 10


class MessageQueueError(Exception):
    """Base exception for message queue errors."""
    pass


# ─── Message ─────────────────────────────────────────────────────────────────


@dataclass
class Message:
    """A message in the queue."""

    body: Any
    headers: Dict[str, Any] = field(default_factory=dict)
    queue_name: Optional[str] = None
    correlation_id: Optional[str] = None
    _acked: bool = field(default=False, repr=False)

    def __post_init__(self):
        if "msg_id" not in self.headers:
            self.headers["msg_id"] = str(uuid.uuid4())
        if "timestamp" not in self.headers:
            self.headers["timestamp"] = time.time()
        if self.correlation_id is None:
            self.correlation_id = self.headers.get("correlation_id")

    def ack(self) -> None:
        """Acknowledge successful processing."""
        self._acked = True

    @property
    def is_acked(self) -> bool:
        return self._acked

    def nack(self, requeue: bool = True) -> None:
        """Mark as failed. Optionally requeue."""
        self._acked = False
        if requeue:
            self.headers["nacked"] = True
            self.headers["retry_count"] = self.headers.get("retry_count", 0) + 1

    def reply(self, body: Any) -> None:
        """Set reply body (for RPC pattern)."""
        self.headers["reply"] = body


# ─── Base Queue ───────────────────────────────────────────────────────────────


class _Queue:
    """In-memory FIFO queue."""

    def __init__(self):
        self._messages: List[Message] = []
        self._lock = asyncio.Lock()

    async def put(self, msg: Message) -> None:
        async with self._lock:
            self._messages.append(msg)

    async def get(self) -> Optional[Message]:
        async with self._lock:
            if self._messages:
                return self._messages.pop(0)
            return None

    def __len__(self) -> int:
        return len(self._messages)

    def is_empty(self) -> bool:
        return len(self._messages) == 0


# ─── Priority Queue ───────────────────────────────────────────────────────────


class _PriorityQueue:
    """In-memory priority queue."""

    def __init__(self):
        self._heap: List[tuple] = []
        self._counter = 0
        self._lock = asyncio.Lock()

    async def put(self, msg: Message) -> None:
        priority = msg.headers.get("priority", Priority.NORMAL)
        async with self._lock:
            heapq.heappush(self._heap, (-priority, self._counter, msg))
            self._counter += 1

    async def get(self) -> Optional[Message]:
        async with self._lock:
            if self._heap:
                _, _, msg = heapq.heappop(self._heap)
                return msg
            return None

    def __len__(self) -> int:
        return len(self._heap)

    def is_empty(self) -> bool:
        return len(self._heap) == 0


import heapq


# ─── Disk-Backed Storage ──────────────────────────────────────────────────────


class _DiskQueue:
    """Disk-backed FIFO queue using JSON lines."""

    def __init__(self, path: str):
        self._path = Path(path)
        self._path.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()
        self._counter = 0

    def _read_all(self) -> List[dict]:
        fpath = self._path / "queue.jsonl"
        if not fpath.exists():
            return []
        with open(fpath) as f:
            return [json.loads(line) for line in f if line.strip()]

    def _write_all(self, items: List[dict]) -> None:
        fpath = self._path / "queue.jsonl"
        with open(fpath, "w") as f:
            for item in items:
                f.write(json.dumps(item) + "\n")

    async def put(self, msg: Message) -> None:
        async with self._lock:
            items = self._read_all()
            items.append({"body": msg.body, "headers": msg.headers})
            self._write_all(items)

    async def get(self) -> Optional[Message]:
        async with self._lock:
            items = self._read_all()
            if items:
                item = items.pop(0)
                self._write_all(items)
                return Message(body=item["body"], headers=item.get("headers", {}))
            return None

    def __len__(self) -> int:
        return len(self._read_all())

    def is_empty(self) -> bool:
        return len(self) == 0


# ─── Sync MessageQueue ───────────────────────────────────────────────────────


class MessageQueue:
    """
    In-process message queue with pub/sub, priorities, and dead-letter support.
    """

    def __init__(self, storage: str = "memory", persist_path: Optional[str] = None):
        self._queues: Dict[str, _Queue] = {}
        self._priority_queues: Dict[str, _PriorityQueue] = {}
        self._topics: Dict[str, List[Callable]] = {}
        self._dlq: Dict[str, _Queue] = {}
        self._reply_futures: Dict[str, asyncio.Future] = {}
        self._storage = storage
        self._persist_path = persist_path
        self._async_loop: Optional[asyncio.AbstractEventLoop] = None

    # ─── Queue Operations ──────────────────────────────────────────────────

    def _get_queue(self, name: str) -> _Queue:
        if name not in self._queues:
            if self._storage == "disk" and self._persist_path:
                self._queues[name] = _DiskQueue(
                    os.path.join(self._persist_path, name)
                )
            else:
                self._queues[name] = _Queue()
        return self._queues[name]

    def _get_pqueue(self, name: str) -> _PriorityQueue:
        if name not in self._priority_queues:
            self._priority_queues[name] = _PriorityQueue()
        return self._priority_queues[name]

    def put(
        self,
        queue_name: str,
        body: Any,
        priority: Priority = Priority.NORMAL,
        correlation_id: Optional[str] = None,
    ) -> Message:
        """Enqueue a message to a queue."""
        headers = {
            "priority": priority,
            "correlation_id": correlation_id,
            "queue": queue_name,
        }
        msg = Message(body=body, headers=headers, queue_name=queue_name,
                      correlation_id=correlation_id)
        if priority != Priority.NORMAL:
            asyncio.get_event_loop().run_until_complete(
                self._get_pqueue(queue_name).put(msg)
            )
        else:
            asyncio.get_event_loop().run_until_complete(
                self._get_queue(queue_name).put(msg)
            )
        return msg

    def get(self, queue_name: str, timeout: Optional[float] = None) -> Optional[Message]:
        """Get next message from queue. Blocks until available or timeout."""
        if asyncio.get_event_loop().is_running():
            raise MessageQueueError(
                "Use AsyncMessageQueue.get() in async context"
            )

        pqueue = self._get_pqueue(queue_name)
        if not pqueue.is_empty():
            return asyncio.get_event_loop().run_until_complete(pqueue.get())

        queue = self._get_queue(queue_name)
        if timeout is None:
            while queue.is_empty():
                asyncio.get_event_loop().run_until_complete(asyncio.sleep(0.001))
            return asyncio.get_event_loop().run_until_complete(queue.get())

        elapsed = 0.0
        step = 0.01
        while elapsed < timeout:
            if not queue.is_empty():
                return asyncio.get_event_loop().run_until_complete(queue.get())
            asyncio.get_event_loop().run_until_complete(asyncio.sleep(step))
            elapsed += step
        return None

    # ─── Pub/Sub ────────────────────────────────────────────────────────────

    def subscribe(self, topic: str, handler: Callable[[Message], None]) -> None:
        """Subscribe a handler to a topic."""
        if topic not in self._topics:
            self._topics[topic] = []
        self._topics[topic].append(handler)

    def publish(self, topic: str, body: Any) -> Message:
        """Publish to a topic. All handlers are called."""
        headers = {"topic": topic}
        msg = Message(body=body, headers=headers)
        for handler in self._topics.get(topic, []):
            try:
                handler(msg)
            except Exception:
                pass
        return msg

    def unsubscribe(self, topic: str, handler: Callable) -> None:
        """Remove a handler from a topic."""
        if topic in self._topics:
            self._topics[topic] = [h for h in self._topics[topic] if h is not handler]

    # ─── RPC / Request-Reply ───────────────────────────────────────────────

    def call(
        self, queue_name: str, body: Any, timeout: float = 30.0
    ) -> Optional[Message]:
        """Send a message and wait for reply (RPC pattern)."""
        if asyncio.get_event_loop().is_running():
            raise MessageQueueError("Use AsyncMessageQueue.call() in async context")

        corr_id = str(uuid.uuid4())
        reply_queue = f"_reply_{corr_id}"

        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._reply_futures[corr_id] = future

        self.put(queue_name, body, correlation_id=corr_id)

        def reply_handler(msg: Message):
            if msg.correlation_id == corr_id:
                future.set_result(msg)
                self._reply_futures.pop(corr_id, None)

        self.subscribe(reply_queue, reply_handler)

        try:
            return asyncio.get_event_loop().run_until_complete(
                asyncio.wait_for(asyncio.Future(), timeout=timeout)
            )
        except Exception:
            self._reply_futures.pop(corr_id, None)
            return None

    def reply(self, original_msg: Message, body: Any) -> None:
        """Reply to a call() request."""
        if original_msg.correlation_id:
            reply_queue = f"_reply_{original_msg.correlation_id}"
            self.publish(reply_queue, body)

    # ─── Dead-Letter Queue ─────────────────────────────────────────────────

    def to_dlq(self, queue_name: str, msg: Message) -> None:
        """Move a message to the dead-letter queue."""
        if queue_name not in self._dlq:
            self._dlq[queue_name] = _Queue()
        asyncio.get_event_loop().run_until_complete(
            self._dlq[queue_name].put(msg)
        )

    def get_dlq(self, queue_name: str) -> Optional[Message]:
        """Get from dead-letter queue."""
        if queue_name in self._dlq:
            return asyncio.get_event_loop().run_until_complete(
                self._dlq[queue_name].get()
            )
        return None

    def __len__(self) -> int:
        return sum(len(q) for q in self._queues.values())


# ─── Async MessageQueue ──────────────────────────────────────────────────────


class AsyncMessageQueue(MessageQueue):
    """
    Async in-process message queue with pub/sub.
    """

    async def put_async(
        self,
        queue_name: str,
        body: Any,
        priority: Priority = Priority.NORMAL,
        correlation_id: Optional[str] = None,
    ) -> Message:
        headers = {
            "priority": priority,
            "correlation_id": correlation_id,
            "queue": queue_name,
        }
        msg = Message(
            body=body,
            headers=headers,
            queue_name=queue_name,
            correlation_id=correlation_id,
        )
        if priority != Priority.NORMAL:
            await self._get_pqueue(queue_name).put(msg)
        else:
            await self._get_queue(queue_name).put(msg)
        return msg

    async def get_async(
        self, queue_name: str, timeout: Optional[float] = None
    ) -> Optional[Message]:
        """Async get from queue."""
        pqueue = self._get_pqueue(queue_name)
        if not pqueue.is_empty():
            return await pqueue.get()

        queue = self._get_queue(queue_name)

        if timeout is None:
            while queue.is_empty():
                await asyncio.sleep(0.001)
            return await queue.get()

        try:
            start = asyncio.get_event_loop().time()
            while queue.is_empty():
                remaining = timeout - (asyncio.get_event_loop().time() - start)
                if remaining <= 0:
                    return None
                await asyncio.sleep(min(0.01, remaining))
            return await queue.get()
        except asyncio.CancelledError:
            return None

    async def call_async(
        self, queue_name: str, body: Any, timeout: float = 30.0
    ) -> Optional[Message]:
        """Async RPC call."""
        corr_id = str(uuid.uuid4())
        reply_queue_name = f"_reply_{corr_id}"

        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._reply_futures[corr_id] = future

        reply_handler_called = False

        async def reply_handler(msg: Message):
            nonlocal reply_handler_called
            if not reply_handler_called:
                reply_handler_called = True
                future.set_result(msg)
                self._reply_futures.pop(corr_id, None)

        if reply_queue_name not in self._topics:
            self._topics[reply_queue_name] = []
        self._topics[reply_queue_name].append(reply_handler)

        await self.put_async(queue_name, body, correlation_id=corr_id)

        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            self._reply_futures.pop(corr_id, None)
            return None

    async def subscribe_async(
        self, topic: str, handler: Callable[[Message], Coroutine]
    ) -> None:
        """Subscribe an async handler to a topic."""
        if topic not in self._topics:
            self._topics[topic] = []
        self._topics[topic].append(handler)

    async def publish_async(self, topic: str, body: Any) -> Message:
        """Async publish to a topic."""
        headers = {"topic": topic}
        msg = Message(body=body, headers=headers)
        tasks = []
        for handler in self._topics.get(topic, []):
            try:
                if asyncio.iscoroutinefunction(handler):
                    tasks.append(asyncio.create_task(handler(msg)))
                else:
                    handler(msg)
            except Exception:
                pass
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        return msg

    def handler(
        self,
        queue_name: str,
        dlq: Optional[str] = None,
        max_retries: int = 0,
        backoff: float = 1.0,
    ):
        """Decorator to mark an async function as a queue handler."""

        def decorator(fn: Callable) -> Callable:
            async def wrapper():
                while True:
                    msg = await self.get_async(queue_name, timeout=5.0)
                    if msg is None:
                        continue
                    try:
                        await fn(msg)
                        msg.ack()
                    except Exception as e:
                        retry_count = msg.headers.get("retry_count", 0)
                        if retry_count >= max_retries:
                            if dlq:
                                self.to_dlq(dlq, msg)
                            msg.nack(requeue=False)
                        else:
                            await asyncio.sleep(backoff * (backoff**retry_count))
                            msg.nack(requeue=True)

            asyncio.create_task(wrapper())
            return fn

        return decorator
