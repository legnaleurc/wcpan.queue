import functools
import queue
import threading
from typing import Any, Awaitable, Callable, Iterable, List, Union

from tornado import gen as tg, ioloop as ti
from wcpan.logger import DEBUG, EXCEPTION

from .task import MaybeTask, TerminalTask, FlushTask, regular_call, ensure_task


AwaitCallback = Callable[[Any], None]


class AsyncWorker(object):

    def __init__(self) -> None:
        super(AsyncWorker, self).__init__()

        self._thread = None
        self._ready_lock = threading.Condition()
        self._loop = None
        # FIXME thread safe
        self._queue = queue.PriorityQueue()
        self._tail = {}
        self._tail_lock = threading.Lock()

    @property
    def is_alive(self) -> bool:
        return self._thread and self._thread.is_alive()

    def start(self) -> None:
        if not self.is_alive:
            self._thread = threading.Thread(target=self._run)
            self._thread.start()
        with self._ready_lock:
            if self._loop is None:
                if not self._ready_lock.wait_for(lambda: self._loop is not None, 1):
                    raise WorkerError('timeout start')
                    # FIXME cleanup

    def stop(self) -> None:
        if self.is_alive:
            task = TerminalTask()
            self._queue.put(task)
            self._thread.join()
            self._thread = None

    async def do(self, task: MaybeTask) -> Awaitable[Any]:
        if not self.is_alive:
            raise WorkerError('worker is dead')

        task = ensure_task(task)
        fn = functools.partial(self.do_later, task)
        future = tg.Task(fn)
        id_ = task.id_
        self._update_tail(id_, future)

        rv = await future
        return rv

    def do_later(self, task: MaybeTask, callback: AwaitCallback = None) -> None:
        if not self.is_alive:
            raise WorkerError('worker is dead')

        task = ensure_task(task)

        id_ = task.id_
        self._make_tail(id_, callback)

        self._queue.put(task)

    async def flush(self, filter_: Callable[['Task'], bool]) -> Awaitable[None]:
        task = FlushTask(filter_)
        await self.do(task)

    def flush_later(self, filter_: Callable[['Task'], bool], callback: AwaitCallback = None) -> None:
        task = FlushTask(filter_)
        self.do_later(task, callback)

    def _make_tail(self, id_: int, callback: AwaitCallback) -> None:
        with self._tail_lock:
            self._tail[id_] = None, callback

    def _update_tail(self, id_: int, future: tg.Future) -> None:
        assert id_ in self._tail, 'invalid task id'

        with self._tail_lock:
            _, callback = self._tail[id_]
            assert _ is None, 'replacing existing future'

            self._tail[id_] = future, callback

    def _do_flush(self, task: 'FlushTask') -> None:
        with self._get_internal_mutex():
            q = self._get_internal_queue()
            nq = filter(lambda _: not task(_), q)
            nq = list(nq)
            DEBUG('wcpan.worker') << 'flush:' << 'before' << len(q) << 'after' << len(nq)
            self._set_internal_queue(nq)

    def _get_internal_queue(self) -> Iterable:
        return self._queue.queue

    def _set_internal_queue(self, nq: List) -> None:
        self._queue.queue = nq

    def _get_internal_mutex(self):
        return self._queue.mutex

    def _run(self) -> None:
        with self._ready_lock:
            self._loop = ti.IOLoop()
            self._loop.add_callback(self._process)
            self._ready_lock.notify()
        self._loop.start()
        self._loop.close()
        self._loop = None

    async def _process(self) -> Awaitable[None]:
        while True:
            task = self._queue.get()
            rv = None
            exception = None
            try:
                if isinstance(task, TerminalTask):
                    break
                elif isinstance(task, FlushTask):
                    rv = self._do_flush(task)
                else:
                    rv = await regular_call(task)
            except Exception as e:
                exception = e
            finally:
                self._queue.task_done()
                id_ = task.id_
                with self._tail_lock:
                    future, done = self._tail.get(id_, (None, None))
                    if id_ in self._tail:
                        del self._tail[id_]
                if exception:
                    if future:
                        future.set_exception(exception)
                    else:
                        EXCEPTION('wcpan.worker', exception) << 'uncought exception'
                elif done:
                    done(rv)
                else:
                    DEBUG('wcpan.worker') << 'unused return value:' << rv
        self._loop.stop()


class WorkerError(Exception):

    def __init__(self, message):
        super(WorkerError, self).__init__(message)
