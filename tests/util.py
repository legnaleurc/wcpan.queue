import asyncio
import threading
from unittest import mock as utm

import wcpan.worker as ww


class DummyException(Exception):
    pass


class BackgroundTask(ww.Task):
    def __init__(self, rv=None, p=None, *args, **kwargs):
        super(BackgroundTask, self).__init__()
        self._arg = []
        self._return_value = 42 if rv is None else rv
        self._priority = 0 if p is None else p

    def _call(self, *args, **kwargs):
        self._real_call()
        self._arg.append((args, kwargs))
        return self._return_value

    @property
    def call_count(self):
        return len(self._arg)

    @property
    def called_with(self):
        return self._arg

    @property
    def priority(self):
        return self._priority


class AsyncMixin(BackgroundTask):
    def __init__(self, *args, **kwargs):
        super(AsyncMixin, self).__init__(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        f = asyncio.create_future()
        f.set_result(self._call(*args, **kwargs))
        return f


class SyncMixin(BackgroundTask):
    def __init__(self, *args, **kwargs):
        super(SyncMixin, self).__init__(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        return self._call(*args, **kwargs)


class BlockerMixin(BackgroundTask):
    def __init__(self, *args, **kwargs):
        super(BlockerMixin, self).__init__(*args, **kwargs)
        self._enter = threading.Event()
        self._exit = threading.Event()

    def _real_call(self):
        self._enter.set()
        self._exit.wait()

    def wait_for_enter(self):
        self._enter.wait()

    def continue_for_exit(self):
        self._exit.set()

    def until_called(self):
        self.wait_for_enter()
        self.continue_for_exit()


class NonBlockerMixin(BackgroundTask):
    def __init__(self, *args, **kwargs):
        super(NonBlockerMixin, self).__init__(*args, **kwargs)

    def _real_call(self):
        pass


class AsyncBlocker(AsyncMixin, BlockerMixin):
    def __init__(self, *args, **kwargs):
        super(AsyncBlocker, self).__init__(*args, **kwargs)


class Blocker(SyncMixin, BlockerMixin):
    def __init__(self, *args, **kwargs):
        super(Blocker, self).__init__(*args, **kwargs)


class AsyncNonBlocker(AsyncMixin, NonBlockerMixin):
    def __init__(self, *args, **kwargs):
        super(AsyncNonBlocker, self).__init__(*args, **kwargs)


class NonBlocker(SyncMixin, NonBlockerMixin):
    def __init__(self, *args, **kwargs):
        super(NonBlocker, self).__init__(*args, **kwargs)


class ResultCollector(ww.Task):
    def __init__(self):
        super(ResultCollector, self).__init__()
        self._return_values = []
        self._done = asyncio.Event()

    @property
    def priority(self):
        return -1

    def add(self, rv):
        self._return_values.append(rv)

    def __call__(self):
        self._done.set()

    async def wait(self):
        await self._done.wait()

    @property
    def values(self):
        return self._return_values
