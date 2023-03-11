import asyncio
import unittest

import async_timeout

from wcpan.worker import AsyncQueue
from .util import NonBlocker, ResultCollector


class TestAsyncQueue(unittest.IsolatedAsyncioTestCase):
    async def testImmediatelyShutdown(self):
        async with async_timeout.timeout(0.1):
            async with AsyncQueue(8) as aq:
                pass

    async def testPost(self):
        async with AsyncQueue(1) as aq:
            fn = NonBlocker()
            rc = ResultCollector()

            aq.post(fn)
            aq.post(rc)
            await rc.wait()

        self.assertEqual(fn.call_count, 1)

    async def testPostParallel(self):
        async with AsyncQueue(2) as aq:
            rc = ResultCollector()

            async def wait_one_second():
                await asyncio.sleep(0.25)
                rc.add(42)

            aq.post(wait_one_second)
            aq.post(wait_one_second)

            async with async_timeout.timeout(0.3):
                aq.post(rc)
                await rc.wait()

        self.assertEqual(rc.values, [42, 42])

    async def testFlush(self):
        async with AsyncQueue(1) as aq:
            fn1 = NonBlocker(p=2)
            fn2 = NonBlocker(p=1)
            rc = ResultCollector()

            aq.post(fn1)
            aq.post(fn2)

            aq.flush(lambda t: t.priority == 2)

            aq.post(rc)

            await rc.wait()

        self.assertEqual(fn1.call_count, 0)
        self.assertEqual(fn2.call_count, 1)
