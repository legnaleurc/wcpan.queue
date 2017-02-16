import unittest as ut

from tornado import gen as tg, testing as tt

import wcpan.worker as ww


class TestAsyncWorkerPool(tt.AsyncTestCase):

    def setUp(self):
        super(TestAsyncWorkerPool, self).setUp()
        self._pool = ww.AsyncWorkerPool()

    def tearDown(self):
        self._pool.stop()
        super(TestAsyncWorkerPool, self).tearDown()
        # TODO ensure all workers are gone
