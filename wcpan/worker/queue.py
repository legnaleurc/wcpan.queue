import asyncio
import contextlib as cl
from typing import Callable

from wcpan.logger import DEBUG, EXCEPTION

from .task import regular_call, ensure_task, MaybeTask


class AsyncQueue(object):

    def __init__(self, maximum=None):
        self._max = 1 if maximum is None else maximum
        self._reset()

    async def __aenter__(self):
        self.start()
        return self

    async def __aexit__(self, type_, exc, tb):
        await self.stop()

    def start(self):
        if self._consumer_list:
            return

        loop = asyncio.get_event_loop()
        self._consumer_list = [
            loop.create_task(self._consume())
                for _ in range(self._max)
        ]

    async def stop(self):
        if not self._consumer_list or self._waiting_finish:
            return

        # if all consumers are busy, we need to flush all pending tasks
        self._set_internal_queue([])

        # if some consumers are busy, we need to wait for running tasks
        if not self.idle:
            self._waiting_finish = asyncio.Event()
            await self._waiting_finish.wait()

        # cancel all consumers
        for consumer in self._consumer_list:
            consumer.cancel()

        self._reset()

    def flush(self, filter_: Callable[['Task'], bool]):
        q = self._get_internal_queue()
        nq = filter(lambda _: not filter_(_), q)
        nq = list(nq)
        DEBUG('wcpan.worker') << 'flush:' << 'before' << len(q) << 'after' << len(nq)
        self._set_internal_queue(nq)

    def post(self, task: MaybeTask):
        if self._waiting_finish:
            return

        self.start()

        task = ensure_task(task)
        self._queue.put_nowait(task)

    @property
    def idle(self):
        return self._active_count == 0

    async def _consume(self):
        while True:
            async with self._pop_task() as task, \
                       self._active_guard():
                try:
                    await regular_call(task)
                except Exception as e:
                    EXCEPTION('wcpan.worker', e) << 'uncaught exception'

            if self._waiting_finish and self.idle:
                self._waiting_finish.set()

    def _reset(self):
        self._queue = asyncio.PriorityQueue()
        self._active_count = 0
        self._waiting_finish = None
        self._consumer_list = None

    def _get_internal_queue(self):
        return self._queue._queue

    def _set_internal_queue(self, nq):
        self._queue._queue = nq

    @cl.asynccontextmanager
    async def _active_guard(self):
        self._active_count += 1
        try:
            yield
        finally:
            self._active_count -= 1

    @cl.asynccontextmanager
    async def _pop_task(self):
        task = await self._queue.get()
        try:
            yield task
        finally:
            self._queue.task_done()
