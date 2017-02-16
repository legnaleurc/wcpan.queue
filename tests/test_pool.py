import unittest as ut
from unittest import mock as utm

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

    @tt.gen_test
    def testDoWithSync(self):
        fn = self._createSyncMock()
        rv = yield self._pool.do(fn)
        fn.assert_called_once_with()
        self.assertEqual(rv, 42)

    @tt.gen_test
    def testDoLaterWithSync(self):
        fn = self._createSyncMock()
        self._pool.do_later(fn)
        yield tg.sleep(0.5)
        fn.assert_called_once_with()

    def _createSyncMock(self):
        return utm.Mock(return_value=42)
