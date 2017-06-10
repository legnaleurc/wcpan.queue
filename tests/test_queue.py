from tornado import gen as tg, testing as tt

import wcpan.worker as ww
from . import util as u


class TestAsyncQueue(tt.AsyncTestCase):

    def setUp(self):
        super(TestAsyncQueue, self).setUp()
        self._queue = ww.AsyncQueue()
        self._queue.start()

    def tearDown(self):
        super(TestAsyncQueue, self).tearDown()

    @tt.gen_test
    def testPost(self):
        fn = u.NonBlocker()
        rc = u.ResultCollector()

        self._queue.post(fn)
        self._queue.post(rc)
        yield rc.wait()
        self.assertEqual(fn.call_count, 1)

        yield self._queue.stop()
