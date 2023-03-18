from asyncio import LifoQueue, PriorityQueue, Queue, Task, TaskGroup
from collections.abc import AsyncGenerator, Coroutine
from contextlib import asynccontextmanager
from importlib.metadata import version
from typing import Generic, Self, TypeAlias, TypeVar


__version__ = version(__package__ or __name__)
__all__ = ("AioQueue",)


_T = TypeVar("_T")
AioCoroutine: TypeAlias = Coroutine[None, None, _T]
_SortableJob: TypeAlias = tuple[int, int, AioCoroutine[_T]]
_Queue: TypeAlias = Queue[_SortableJob[_T]]


class AioQueue(Generic[_T]):
    @classmethod
    def fifo(cls, maxsize: int = 0) -> Self:
        return AioQueue[_T](Queue(maxsize))

    @classmethod
    def priority(cls, maxsize: int = 0) -> Self:
        return AioQueue[_T](PriorityQueue(maxsize))

    @classmethod
    def lifo(cls, maxsize: int = 0) -> Self:
        return AioQueue[_T](LifoQueue(maxsize))

    def __init__(self, queue: _Queue[_T]) -> None:
        self._id = 0
        self._queue = queue

    @property
    def maxsize(self) -> int:
        return self._queue.maxsize

    @property
    def size(self) -> int:
        return self._queue.qsize()

    @property
    def empty(self) -> bool:
        return self._queue.empty()

    @property
    def full(self) -> bool:
        return self._queue.full()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, e, et, tb) -> None:
        self.purge()

    async def push(self, coro: AioCoroutine[_T], priority: int = 0) -> None:
        self._id = self._id + 1
        await self._queue.put((priority, self._id, coro))

    def push_nowait(self, coro: AioCoroutine[_T], priority: int = 0) -> None:
        self._id = self._id + 1
        self._queue.put_nowait((priority, self._id, coro))

    async def consume(self, maxsize: int = 1) -> None:
        if self.empty:
            raise ValueError(f"queue must have something to consume")

        if maxsize <= 0:
            raise ValueError(f"invalid consumer size: {maxsize}")

        async with _spawn(maxsize, _consume(self._queue)):
            await self._queue.join()

    async def collect(self, maxsize: int = 1) -> AsyncGenerator[_T, None]:
        if self.empty:
            raise ValueError(f"queue must have something to collect")

        if maxsize <= 0:
            raise ValueError(f"invalid collector size: {maxsize}")

        result = Queue[_T]()
        async with _spawn(maxsize, _collect(self._queue, result)) as group:
            producer = group.create_task(self._queue.join())
            consumer = group.create_task(result.join())
            while not producer.done() or not result.empty():
                rv = await result.get()
                try:
                    yield rv
                finally:
                    result.task_done()
            await consumer

    def purge(self) -> None:
        while not self._queue.empty():
            (_p, _i, cb) = self._queue.get_nowait()
            cb.close()
            self._queue.task_done()


@asynccontextmanager
async def _spawn(maxsize: int, worker: AioCoroutine):
    async with TaskGroup() as group:
        task_list: list[Task[None]] = []
        while maxsize > 0:
            task = group.create_task(worker)
            task_list.append(task)
            maxsize -= 1
        try:
            yield group
        finally:
            for task in task_list:
                task.cancel()


async def _consume(q: _Queue[_T]) -> None:
    while True:
        (_p, _i, cb) = await q.get()
        try:
            await cb
        finally:
            q.task_done()


async def _collect(iq: _Queue[_T], oq: Queue[_T]) -> None:
    while True:
        (_p, _i, cb) = await iq.get()
        try:
            rv = await cb
            await oq.put(rv)
        finally:
            iq.task_done()
