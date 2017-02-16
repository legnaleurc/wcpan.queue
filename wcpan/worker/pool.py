import multiprocessing as mp
import functools as ft

import tornado.ioloop as ti
import tornado.locks as tl

from .worker import AsyncWorker, MaybeTask, AwaitCallback


class AsyncWorkerPool(object):

    def __init__(self, workers: int = None) -> None:
        self._idle = []
        self._busy = set()
        self._max = mp.cpu_count() if workers is None else workers
        self._lock = tl.Condition()

    def start(self) -> None:
        pass

    def stop(self) -> None:
        for worker in self._idle:
            worker.stop()
        self._idle = []
        for worker in self._busy:
            worker.stop()
        self._busy = set()

    @property
    def is_alive(self) -> bool:
        return True

    async def do(self, task: MaybeTask) -> Awaitable[Any]:
        async with WorkerRecycler(self) as worker:
            rv = await worker.do(task)
            return rv

    def do_later(self, task: MaybeTask, callback: AwaitCallback = None) -> None:
        loop = ti.current()
        fn = ft.partial(self._do_later_internal, task, callback)
        loop.add_callback(fn)

    async def _do_later_internal(self, task: MaybeTask,
                                 callback: AwaitCallback = None) -> None:
        async with WorkerRecycler(self) as worker:
            worker.do_later(task, callback)

    async def _get_worker(self) -> Awaitable[AsyncWorker]:
        while True:
            worker = self._try_get_or_create()
            if worker:
                self._busy.add(worker)
                return worker

            # no worker available, wait for idle
            await self._lock.wait()

    def _recycle(self, worker: AsyncWorker) -> None:
        self._busy.remove(worker)
        self._idle.append(worker)
        self._lock.notify()

    def _try_get_or_create(self) -> Optional[AsyncWorker]:
        worker = None
        if self._idle:
            worker = self._idle.pop(0)
        elif len(self._busy) < self._max:
            worker = AsyncWorker()
        return worker


class WorkerRecycler(object):

    def __init__(self, pool: AsyncWorkerPool) -> None:
        self._pool = pool
        self._worker = None

    async def __aenter__(self) -> Awaitable[AsyncWorker]:
        self._worker = await self._pool._get_worker()
        return self._worker

    async def __aexit__(self, *args, **kwargs) -> Awaitable[None]:
        self._pool._recycle(self._worker)
