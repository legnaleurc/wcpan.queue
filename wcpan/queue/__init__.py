from asyncio import LifoQueue, PriorityQueue, Queue, Task, TaskGroup
from importlib.metadata import version
from typing import Coroutine, Self, TypeAlias


__version__ = version(__package__ or __name__)
__all__ = ("AioQueue",)


Runnable: TypeAlias = Coroutine[None, None, None]
_SortableRunnable: TypeAlias = tuple[int, int, Runnable]
_Queue: TypeAlias = Queue[_SortableRunnable]


class AioQueue:
    @classmethod
    def fifo(cls, maxsize: int = 0) -> Self:
        return AioQueue(Queue(maxsize))

    @classmethod
    def priority(cls, maxsize: int = 0) -> Self:
        return AioQueue(PriorityQueue(maxsize))

    @classmethod
    def lifo(cls, maxsize: int = 0) -> Self:
        return AioQueue(LifoQueue(maxsize))

    def __init__(self, queue: _Queue) -> None:
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

    async def push(self, runnable: Runnable, priority: int = 0) -> None:
        self._id = self._id + 1
        await self._queue.put((priority, self._id, runnable))

    def push_nowait(self, runnable: Runnable, priority: int = 0) -> None:
        self._id = self._id + 1
        self._queue.put_nowait((priority, self._id, runnable))

    async def consume(self, maxsize: int = 1) -> None:
        if self._queue.empty():
            raise ValueError(f"queue must have something to consume")

        if maxsize <= 0:
            raise ValueError(f"invalid consumer size: {maxsize}")

        async with TaskGroup() as tg:
            consumer_list: list[Task[None]] = []
            while maxsize > 0:
                task = tg.create_task(_consume(self._queue))
                consumer_list.append(task)
                maxsize -= 1
            try:
                await self._queue.join()
            finally:
                for task in consumer_list:
                    task.cancel()

    def purge(self) -> None:
        while not self._queue.empty():
            (_p, _i, cb) = self._queue.get_nowait()
            cb.close()
            self._queue.task_done()


async def _consume(q: _Queue) -> None:
    while True:
        (_p, _i, cb) = await q.get()
        try:
            await cb
        finally:
            q.task_done()
