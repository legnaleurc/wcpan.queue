import asyncio
import contextlib as cl
import multiprocessing as mp
from typing import Callable

from wcpan.logger import DEBUG, EXCEPTION

from .task import regular_call, ensure_task, MaybeTask


class AsyncQueue(object):

    def __init__(self, maximum=None):
        self._max = mp.cpu_count() if maximum is None else maximum
        self._waiting_idle = asyncio.Condition()
        self._reset()

    async def __aenter__(self):
        return self

    async def __aexit__(self, type_, exc, tb):
        await self.shutdown()

    async def join(self):
        if self.idle:
            return
        async with self._waiting_idle:
            await self._waiting_idle.wait()

    async def shutdown(self):
        # no consumer means it never used
        if not self._consumer_list or self._shutting_down:
            return

        self._shutting_down = True

        # if some consumers are busy, we need to wait for running tasks
        await self.join()

        # cancel all consumers
        for consumer in self._consumer_list:
            consumer.cancel()
        await asyncio.wait(self._consumer_list)

        self._reset()

    def flush(self, filter_: Callable[['Task'], bool] = None):
        q = self._get_internal_queue()
        if filter_ is not None:
            nq = [_ for _ in q if not filter_(_)]
        else:
            nq = []
        DEBUG('wcpan.worker') << 'flush:' << 'before' << len(q) << 'after' << len(nq)
        self._set_internal_queue(nq)

    def post(self, task: MaybeTask):
        if self._shutting_down:
            return

        self._start()

        task = ensure_task(task)
        self._queue.put_nowait(task)

    @property
    def idle(self):
        q = self._get_internal_queue()
        return not q and self._active_count == 0

    def _start(self):
        if self._consumer_list:
            return

        loop = asyncio.get_running_loop()
        self._consumer_list = [
            loop.create_task(self._consume())
                for _ in range(self._max)
        ]

    async def _consume(self):
        while True:
            async with self._pop_task() as task, \
                       self._active_guard():
                try:
                    await regular_call(task)
                except Exception as e:
                    EXCEPTION('wcpan.worker', e) << 'uncaught exception'

            async with self._waiting_idle:
                if self.idle:
                    self._waiting_idle.notify_all()

    def _reset(self):
        self._queue = asyncio.PriorityQueue()
        self._active_count = 0
        self._shutting_down = False
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
