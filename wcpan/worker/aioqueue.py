from asyncio import CancelledError, Queue, Task, TaskGroup
from collections.abc import Coroutine
from logging import getLogger
from typing import TypeAlias


Callback: TypeAlias = Coroutine[None, None, None]
AioQueue: TypeAlias = Queue[Callback | None]


def create_queue(maxsize: int = 0) -> AioQueue:
    return Queue(maxsize)


async def consume(q: AioQueue) -> None:
    while True:
        cb = await q.get()
        if not cb:
            break
        try:
            await cb
        finally:
            q.task_done()


async def consume_all(q: AioQueue, maxsize: int = 1):
    if q.empty():
        raise ValueError(f"queue must have something to consume")

    if maxsize <= 0:
        raise ValueError(f"invalid consumer size: {maxsize}")

    async with TaskGroup() as tg:
        consumer_list: list[Task[None]] = []
        while maxsize > 0:
            task = tg.create_task(consume(q))
            consumer_list.append(task)
            maxsize -= 1

        await q.join()
        for task in consumer_list:
            task.cancel()
            try:
                await task
            except CancelledError:
                # have to consume the exception
                getLogger(__name__).debug("consumer cancelled")
