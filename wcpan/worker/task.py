import functools
import inspect
import itertools
from typing import Any, Awaitable, Callable, Union


RawTask = Callable[[], Any]
MaybeTask = Union['Task', RawTask]


@functools.total_ordering
class Task(object):

    __counter = itertools.count()

    def __init__(self, callable_: RawTask = None) -> None:
        super(Task, self).__init__()

        self.__callable = callable_
        # FIXME atomic because GIL
        self.__id = next(self.__counter)

    def __eq__(self, that: 'Task') -> bool:
        return self.equal(that)

    def __lt__(self, that: 'Task') -> bool:
        return self.higher_then(that)

    def __call__(self) -> Any:
        if not self.__callable:
            raise NotImplementedError()
        return self.__callable()

    # highest first
    @property
    def priority(self) -> int:
        return 0

    @property
    def id_(self) -> int:
        return self.__id

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


def ensure_task(maybe_task: MaybeTask) -> Task:
    if not isinstance(maybe_task, Task):
        maybe_task = Task(maybe_task)
    return maybe_task


async def regular_call(task: Task) -> Awaitable[Any]:
    rv = task()
    if inspect.isawaitable(rv):
        rv = await rv
    return rv
