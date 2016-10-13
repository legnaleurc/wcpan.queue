import functools
import inspect
import itertools
import threading
from typing import Any, Awaitable, Callable, Union

from tornado import gen as tg, ioloop as ti, queues as tq
from wcpan.logger import DEBUG


RawTask = Callable[[], Any]
MaybeTask = Union['Task', RawTask]
AwaitCallback = Callable[[Any], None]


class AsyncWorker(object):

    def __init__(self) -> None:
        super(AsyncWorker, self).__init__()

        self._thread = None
        self._ready_lock = threading.Condition()
        self._loop = None
        # FIXME thread safe
        self._queue = tq.PriorityQueue()
        self._tail = {}

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
                    raise Exception('timeout')

    def stop(self) -> None:
        if self._loop is not None:
            self._loop.add_callback(self._loop.stop)
        if self.is_alive:
            self._thread.join()
            self._thread = None

    async def do(self, task: MaybeTask) -> Awaitable[Any]:
        task = ensure_task(task)
        id_ = id(task)
        future = tg.Task(functools.partial(self._make_tail, id_))
        self._update_tail(id_, future)

        await self._queue.put(task)
        rv = await future
        return rv

    def do_later(self, task: MaybeTask, callback: AwaitCallback = None) -> None:
        if callback:
            fn = functools.partial(self._wrapped_do, task, callback)
        else:
            fn = functools.partial(self.do, task)
        loop = ti.IOLoop.current()
        loop.add_callback(fn)

    def flush(self, filter_: Callable[['Task'], bool]) -> None:
        task = FlushTask(filter_)
        self.do_later(task)

    async def _wrapped_do(self, task: MaybeTask, callback: AwaitCallback) -> None:
        rv = await self.do(task)
        callback(rv)

    def _make_tail(self, id_: int, callback: AwaitCallback) -> None:
        self._tail[id_] = callback

    def _update_tail(self, id_: int, future: tg.Future) -> None:
        cb = self._tail[id_]
        self._tail[id_] = (future, cb)

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
            task = await self._queue.get()

            if isinstance(task, FlushTask):
                self._queue.task_done()

                queue = filter(lambda _: not task(_), self._queue._queue)
                queue = list(queue)
                DEBUG('wcpan.worker') << 'flush:' << 'before' << len(self._queue._queue) << 'after' << len(queue)
                self._queue._queue = queue

                continue

            rv = None
            exception = None
            try:
                rv = task()
                if inspect.isawaitable(rv):
                    rv = await rv
            except Exception as e:
                exception = e
            finally:
                self._queue.task_done()
                id_ = id(task)
                future, done = self._tail.get(id_, (None, None))
                if future or done:
                    del self._tail[id_]
                if exception and future:
                    future.set_exception(exception)
                elif done:
                    done(rv)


@functools.total_ordering
class Task(object):

    _counter = itertools.count()

    def __init__(self, callable_: RawTask = None) -> None:
        super(Task, self).__init__()

        self._callable = callable_
        # FIXME atomic because GIL
        self._id = next(self._counter)

    def __eq__(self, that: 'Task') -> bool:
        if not isinstance(that, self.__class__):
            return NotImplemented
        return self.equal(that)

    def __gt__(self, that: 'Task') -> bool:
        if not isinstance(that, self.__class__):
            return NotImplemented
        return not self.higher_then(that)

    def __call__(self) -> Any:
        if not self._callable:
            raise NotImplementedError()
        return self._callable()

    # highest first
    @property
    def priority(self) -> int:
        return 0

    @property
    def id_(self) -> int:
        return self._id

    def equal(self, that: 'Task') -> bool:
        return self.priority == that.priority and self.id_ == that.id_

    def higher_then(self, that: 'Task') -> bool:
        if self.priority > that.priority:
            return True
        if self.priority < that.priority:
            return False
        # lower ID was created earlier
        return self.id_ < that.id_


# ATTENTION DO NOT inherit this class
class FlushTask(Task):

    def __init__(self, filter_: Callable[[Task], bool]) -> None:
        super(FlushTask, self).__init__()

        self._filter = filter_

    def __call__(self, task: Task) -> bool:
        return self._filter(task)

    def __eq__(self, that: 'Task') -> bool:
        rv = self.equal(that)
        return rv

    def __gt__(self, that: 'Task') -> bool:
        rv = not self.higher_then(that)
        return rv

    def equal(self, that: 'Task') -> bool:
        if isinstance(that, self.__class__):
            return self.id_ == that.id_
        return False

    def higher_then(self, that: 'Task') -> bool:
        if not isinstance(that, self.__class__):
            return True
        # lower ID was created earlier
        return self.id_ < that.id_


def ensure_task(maybe_task: MaybeTask) -> 'Task':
    if not isinstance(maybe_task, Task):
        maybe_task = Task(maybe_task)
    return maybe_task
