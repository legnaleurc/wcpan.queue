import unittest as ut

from tornado import testing as tt

import wcpan.worker as ww
from . import util as u


class TestAsyncWorkerPool(tt.AsyncTestCase):

    def setUp(self):
        super(TestAsyncWorkerPool, self).setUp()
        self._pool = ww.AsyncWorkerPool()

    def tearDown(self):
        self._pool.stop()
        super(TestAsyncWorkerPool, self).tearDown()
        # TODO ensure all workers are gone

    @tt.gen_test
    def testDoWithSync(self):
        fn = u.NonBlocker()
        rv = yield self._pool.do(fn)
        self.assertEqual(fn.call_count, 1)
        self.assertEqual(rv, 42)

    @tt.gen_test
    def testDoLaterWithSync(self):
        fn = u.NonBlocker()
        rc = u.ResultCollector()
        self._pool.do_later(fn)
        self._pool.do_later(rc)
        yield rc.wait()
        self.assertEqual(fn.call_count, 1)
