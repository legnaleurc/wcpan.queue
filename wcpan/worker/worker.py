import functools
import inspect
import itertools
import queue
import threading
from typing import Any, Awaitable, Callable, Iterable, List, Union

from tornado import gen as tg, ioloop as ti
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
                    raise Exception('timeout')

    def stop(self) -> None:
        if self.is_alive:
            task = TerminalTask()
            self._queue.put(task)
            self._thread.join()
            self._thread = None

    async def do(self, task: MaybeTask) -> Awaitable[Any]:
        task = ensure_task(task)

        future = tg.Task(functools.partial(self.do_later, task))

        id_ = task.id_
        self._update_tail(id_, future)

        rv = await future
        return rv

    def do_later(self, task: MaybeTask, callback: AwaitCallback = None) -> None:
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
                if exception and future:
                    future.set_exception(exception)
                elif done:
                    done(rv)
        self._loop.stop()


@functools.total_ordering
class Task(object):

    _counter = itertools.count()

    def __init__(self, callable_: RawTask = None) -> None:
        super(Task, self).__init__()

        self._callable = callable_
        # FIXME atomic because GIL
        self._id = next(self._counter)

    def __eq__(self, that: 'Task') -> bool:
        return self.equal(that)

    def __lt__(self, that: 'Task') -> bool:
        return self.higher_then(that)

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
        if not isinstance(that, self.__class__):
            return NotImplemented
        return self.id_ == that.id_

    def higher_then(self, that: 'Task') -> bool:
        if not isinstance(that, self.__class__):
            return NotImplemented
        if self.priority > that.priority:
            return True
        if self.priority < that.priority:
            return False
        # lower ID was created earlier
        return self.id_ < that.id_


# ATTENTION DO NOT use this class
class InternalTask(Task):

    def __init__(self, *args, **kwargs) -> None:
        super(InternalTask, self).__init__(*args, **kwargs)

    def equal(self, that: Task) -> bool:
        # if not same type, always non-equal
        if self.__class__ is not that.__class__:
            return False
        return self.id_ == that.id_

    def higher_then(self, that: Task) -> bool:
        # if not internal task, it has lower priority
        if not isinstance(that, InternalTask):
            return True
        if self.priority > that.priority:
            return True
        if self.priority < that.priority:
            return False
        # lower ID was created earlier
        return self.id_ < that.id_


# ATTENTION DO NOT use this class
class FlushTask(InternalTask):

    def __init__(self, filter_: Callable[[Task], bool]) -> None:
        super(FlushTask, self).__init__(callable_=filter_)

    def __call__(self, task: Task) -> bool:
        return self._callable(task)

    @property
    def priority(self) -> int:
        return 1


# ATTENTION DO NOT use this class
class TerminalTask(InternalTask):

    def __init__(self) -> None:
        super(TerminalTask, self).__init__()

    @property
    def priority(self) -> int:
        return 2


def ensure_task(maybe_task: MaybeTask) -> Task:
    if not isinstance(maybe_task, Task):
        maybe_task = Task(maybe_task)
    return maybe_task


async def regular_call(task: Task) -> Awaitable[Any]:
    rv = task()
    if inspect.isawaitable(rv):
        rv = await rv
    return rv
