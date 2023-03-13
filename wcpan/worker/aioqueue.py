from asyncio import CancelledError, Queue, Task, TaskGroup
from collections.abc import Coroutine
from logging import getLogger
from typing import TypeAlias


Callback: TypeAlias = Coroutine[None, None, None]
AioQueue: TypeAlias = Queue[Callback]


def create_queue(maxsize: int = 0) -> AioQueue:
    return Queue(maxsize)


async def _consume(q: AioQueue) -> None:
    while True:
        cb = await q.get()
        try:
            await cb
        finally:
            q.task_done()


async def consume_all(q: AioQueue, maxsize: int = 1) -> None:
    if q.empty():
        raise ValueError(f"queue must have something to consume")

    if maxsize <= 0:
        raise ValueError(f"invalid consumer size: {maxsize}")

    async with TaskGroup() as tg:
        consumer_list: list[Task[None]] = []
        while maxsize > 0:
            task = tg.create_task(_consume(q))
            consumer_list.append(task)
            maxsize -= 1

        try:
            await q.join()
        finally:
            await _cancel_all(consumer_list)


async def _cancel_all(task_list: list[Task]) -> None:
    for task in task_list:
        task.cancel()
        try:
            await task
        except CancelledError:
            # have to consume the exception
            getLogger(__name__).debug("consumer cancelled")


def purge_queue(q: AioQueue) -> None:
    getLogger(__name__).debug(f"purge {q.qsize()} items in the queue")
    while not q.empty():
        coro = q.get_nowait()
        coro.close()
        q.task_done()
