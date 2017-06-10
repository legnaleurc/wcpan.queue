from tornado import queues as tq, locks as tl, ioloop as ti
from wcpan.logger import EXCEPTION

from .task import regular_call, ensure_task, MaybeTask, TerminalTask


class AsyncQueue(object):

    def __init__(self, maximum=None):
        self._max = 1 if maximum is None else maximum
        self._queue = tq.PriorityQueue()
        self._lock = tl.Semaphore(self._max)
        self._loop = ti.IOLoop.current()
        self._running = False
        self._end = tl.Event()

    def start(self):
        if self._running:
            return
        self._loop.add_callback(self._run)
        self._running = True

    async def stop(self):
        task = TerminalTask()
        self._running = False
        for i in range(self._max):
            self._queue.put_nowait(task)
        await self._end.wait()

    def post(self, task: MaybeTask):
        task = ensure_task(task)
        self._queue.put_nowait(task)

    async def _run(self):
        while self._running:
            async with self._lock:
                task = await self._queue.get()
                try:
                    if isinstance(task, TerminalTask):
                        break
                    else:
                        await regular_call(task)
                except Exception as e:
                    EXCEPTION('wcpan.worker', e) << 'uncaught exception'
                finally:
                    self._queue.task_done()
        self._end.set()
