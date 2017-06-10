import functools as ft

from tornado import queues as tq, locks as tl, ioloop as ti
from wcpan.logger import EXCEPTION

from .task import regular_call, ensure_task, MaybeTask, TerminalTask


class AsyncQueue(object):

    def __init__(self, maximum=None):
        self._max = 1 if maximum is None else maximum
        self._lock = tl.Semaphore(self._max)
        self._loop = ti.IOLoop.current()
        self._running = False

        self._reset()

    def start(self):
        if self._running:
            return
        self._loop.add_callback(self._process)
        self._running = True

    async def stop(self):
        task = TerminalTask()
        self._running = False
        for i in range(self._max):
            self._queue.put_nowait(task)
        self._end = tl.Event()
        await self._end.wait()
        self._reset()

    def post(self, task: MaybeTask):
        task = ensure_task(task)
        self._queue.put_nowait(task)

    async def _process(self):
        while self._running:
            await self._lock.acquire()
            task = await self._queue.get()
            fn = ft.partial(self._run, task)
            self._loop.add_callback(fn)

    async def _run(self, task):
        try:
            if isinstance(task, TerminalTask):
                return
            else:
                await regular_call(task)
        except Exception as e:
            EXCEPTION('wcpan.worker', e) << 'uncaught exception'
        finally:
            self._queue.task_done()
            self._lock.release()
            if self._end and self._lock._value == self._max:
                self._end.set()

    def _reset(self):
        self._queue = tq.PriorityQueue()
        self._end = None
