import functools
import inspect
import itertools
from typing import Any, Awaitable, Callable, Union


RawTask = Callable[[], Any]
MaybeTask = Union['Task', RawTask]


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
        if not isinstance(that, Task):
            return NotImplemented
        return self.id_ == that.id_

    def higher_then(self, that: 'Task') -> bool:
        if not isinstance(that, Task):
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
        return super(InternalTask, self).equal(that)

    def higher_then(self, that: Task) -> bool:
        # if not internal task, it has lower priority
        if not isinstance(that, InternalTask):
            return True
        return super(InternalTask, self).higher_then(that)


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
