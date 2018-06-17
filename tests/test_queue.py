import asyncio
import time
import unittest as ut

import wcpan.worker as ww
from . import util as u


class TestAsyncQueue(ut.TestCase):

    @ww.sync
    async def testPost(self):
        self._queue = ww.AsyncQueue()
        self._queue.start()

        fn = u.NonBlocker()
        rc = u.ResultCollector()

        self._queue.post(fn)
        self._queue.post(rc)
        await rc.wait()
        await self._queue.stop()

        self.assertEqual(fn.call_count, 1)

    @ww.sync
    async def testPostParallel(self):
        self._queue = ww.AsyncQueue(2)
        self._queue.start()

        rc = u.ResultCollector()
        async def wait_one_second():
            await asyncio.sleep(0.25)
            rc.add(42)

        self._queue.post(wait_one_second)
        self._queue.post(wait_one_second)

        before = time.perf_counter()
        self._queue.post(rc)
        await rc.wait()
        after = time.perf_counter()
        diff = after - before
        await self._queue.stop()

        self.assertLess(diff, 0.3)
        self.assertEqual(rc.values, [42, 42])

    @ww.sync
    async def testFlush(self):
        self._queue = ww.AsyncQueue()
        self._queue.start()

        fn1 = u.NonBlocker(p=2)
        fn2 = u.NonBlocker(p=1)
        rc = u.ResultCollector()

        self._queue.post(fn1)
        self._queue.post(fn2)

        self._queue.flush(lambda t: t.priority == 2)

        self._queue.post(rc)

        await rc.wait()
        await self._queue.stop()

        self.assertEqual(fn1.call_count, 0)
        self.assertEqual(fn2.call_count, 1)
