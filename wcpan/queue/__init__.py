from asyncio import LifoQueue, PriorityQueue, Queue, Task, TaskGroup
from collections.abc import AsyncGenerator, Coroutine, Callable
from contextlib import asynccontextmanager
from functools import partial
from importlib.metadata import version
from typing import Self


__version__ = version(__package__ or __name__)
__all__ = ("AioQueue",)


type AioCoroutine[T] = Coroutine[None, None, T]
type _SortableJob[T] = tuple[int, int, AioCoroutine[T]]
type _Queue[T] = Queue[_SortableJob[T]]
type _Worker[T] = Callable[[], AioCoroutine[T]]


class AioQueue[T]:
    @classmethod
    def fifo(cls, maxsize: int = 0) -> Self:
        return AioQueue[T](Queue(maxsize))

    @classmethod
    def priority(cls, maxsize: int = 0) -> Self:
        return AioQueue[T](PriorityQueue(maxsize))

    @classmethod
    def lifo(cls, maxsize: int = 0) -> Self:
        return AioQueue[T](LifoQueue(maxsize))

    def __init__(self, queue: _Queue[T]) -> None:
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

    def __exit__(self, et: object, e: object, tb: object) -> None:
        self.purge()

    async def push(self, coro: AioCoroutine[T], priority: int = 0) -> None:
        self._id = self._id + 1
        await self._queue.put((priority, self._id, coro))

    def push_nowait(self, coro: AioCoroutine[T], priority: int = 0) -> None:
        self._id = self._id + 1
        self._queue.put_nowait((priority, self._id, coro))

    async def consume(self, maxsize: int = 1) -> None:
        if self.empty:
            raise ValueError(f"queue must have something to consume")

        if maxsize <= 0:
            raise ValueError(f"invalid consumer size: {maxsize}")

        async with _spawn(maxsize, partial(_consume, self._queue)):
            await self._queue.join()

    async def collect(self, maxsize: int = 1) -> AsyncGenerator[T, None]:
        if self.empty:
            raise ValueError(f"queue must have something to collect")

        if maxsize <= 0:
            raise ValueError(f"invalid collector size: {maxsize}")

        result = Queue[T]()
        async with _spawn(maxsize, partial(_collect, self._queue, result)) as group:
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
async def _spawn[T](maxsize: int, worker: _Worker[T]):
    async with TaskGroup() as group:
        task_list: list[Task[T]] = []
        while maxsize > 0:
            task = group.create_task(worker())
            task_list.append(task)
            maxsize -= 1

        yield group

        for task in task_list:
            if not task.done():
                task.cancel()


async def _consume[T](q: _Queue[T]) -> None:
    while True:
        (_p, _i, cb) = await q.get()
        try:
            await cb
        finally:
            q.task_done()


async def _collect[T](iq: _Queue[T], oq: Queue[T]) -> None:
    while True:
        (_p, _i, cb) = await iq.get()
        try:
            rv = await cb
            await oq.put(rv)
        finally:
            iq.task_done()
