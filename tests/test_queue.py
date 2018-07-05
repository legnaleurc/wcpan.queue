import asyncio
import time
import unittest as ut

import async_timeout as at

import wcpan.worker as ww
from . import util as u


class TestAsyncQueue(ut.TestCase):

    @ww.sync
    async def testImmediatelyShutdown(self):
        with at.timeout(0.1):
            async with ww.AsyncQueue(8) as aq:
                pass

    @ww.sync
    async def testPost(self):
        async with ww.AsyncQueue() as aq:
            fn = u.NonBlocker()
            rc = u.ResultCollector()

            aq.post(fn)
            aq.post(rc)
            await rc.wait()

        self.assertEqual(fn.call_count, 1)

    @ww.sync
    async def testPostParallel(self):
        async with ww.AsyncQueue(2) as aq:
            rc = u.ResultCollector()
            async def wait_one_second():
                await asyncio.sleep(0.25)
                rc.add(42)

            aq.post(wait_one_second)
            aq.post(wait_one_second)

            before = time.perf_counter()
            aq.post(rc)
            await rc.wait()
            after = time.perf_counter()
            diff = after - before

        self.assertLess(diff, 0.3)
        self.assertEqual(rc.values, [42, 42])

    @ww.sync
    async def testFlush(self):
        async with ww.AsyncQueue() as aq:
            fn1 = u.NonBlocker(p=2)
            fn2 = u.NonBlocker(p=1)
            rc = u.ResultCollector()

            aq.post(fn1)
            aq.post(fn2)

            aq.flush(lambda t: t.priority == 2)

            aq.post(rc)

            await rc.wait()

        self.assertEqual(fn1.call_count, 0)
        self.assertEqual(fn2.call_count, 1)
