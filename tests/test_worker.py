import functools
import unittest as ut
from unittest import mock as utm

from tornado import gen as tg, testing as tt

import wcpan.worker as worker
from . import util as u


class TestAsyncWorker(tt.AsyncTestCase):

    def setUp(self):
        super(TestAsyncWorker, self).setUp()
        self._worker = worker.AsyncWorker()
        self._worker.start()
        self.assertTrue(self._worker.is_alive)

    def tearDown(self):
        self._worker.stop()
        super(TestAsyncWorker, self).tearDown()
        self.assertFalse(self._worker.is_alive)

    @utm.patch('tornado.ioloop.IOLoop', autospec=True)
    @utm.patch('threading.Condition', autospec=True)
    @utm.patch('threading.Thread', autospec=True)
    def testStartTwice(self, FakeThread, FakeCondition, FakeIOLoop):
        w = worker.AsyncWorker()
        w.start()

        the_loop = w._loop
        w.start()
        w._thread.start.assert_called_once_with()
        self.assertEqual(the_loop, w._loop)

    @utm.patch('tornado.ioloop.IOLoop', autospec=True)
    @utm.patch('threading.Condition', autospec=True)
    @utm.patch('threading.Thread', autospec=True)
    def testStopTwice(self, FakeThread, FakeCondition, FakeIOLoop):
        w = worker.AsyncWorker()
        w.start()
        w.stop()

        w.stop()
        self.assertIsNone(w._thread)
        self.assertIsNone(w._loop)

    @tt.gen_test
    def testDoWithSync(self):
        fn = self._createSyncMock()
        rv = yield self._worker.do(fn)
        fn.assert_called_once_with()
        self.assertEqual(rv, 42)

    @tt.gen_test
    def testDoWithAsync(self):
        fn = self._createAsyncMock()
        rv = yield self._worker.do(fn)
        fn.assert_called_once_with()
        fn.assert_awaited()
        self.assertEqual(rv, 42)

    @tt.gen_test
    def testDoLaterWithSync(self):
        fn = self._createSyncMock()
        self._worker.do_later(fn)
        yield tg.sleep(0.5)
        fn.assert_called_once_with()

    @tt.gen_test
    def testDoLaterWithAsync(self):
        fn = self._createAsyncMock()
        self._worker.do_later(fn)
        yield tg.sleep(0.5)
        fn.assert_called_once_with()

    @tt.gen_test
    def testDoWithSyncPartial(self):
        fn = self._createSyncMock()
        rv = yield self._worker.do(functools.partial(fn, 1, k=7))
        fn.assert_called_once_with(1, k=7)
        self.assertEqual(rv, 42)

    @tt.gen_test
    def testDoWithAsyncPartial(self):
        fn = self._createAsyncMock()
        rv = yield self._worker.do(functools.partial(fn, 1, k=7))
        fn.assert_called_once_with(1, k=7)
        fn.assert_awaited()
        self.assertEqual(rv, 42)

    @tt.gen_test
    def testDoLaterWithSyncPartial(self):
        fn = self._createSyncMock()
        self._worker.do_later(functools.partial(fn, 1, k=7))
        yield tg.sleep(0.5)
        fn.assert_called_once_with(1, k=7)

    @tt.gen_test
    def testDoLaterWithAsyncPartial(self):
        fn = self._createAsyncMock()
        self._worker.do_later(functools.partial(fn, 1, k=7))
        yield tg.sleep(0.5)
        fn.assert_called_once_with(1, k=7)
        fn.assert_awaited()

    @tt.gen_test
    def testRunOrder(self):
        first_task = self._createAsyncMock()
        side = []
        second_task = FakeTask(side, 2)
        third_task = FakeTask(side, 3)
        self._worker.do_later(first_task)
        self._worker.do_later(second_task)
        self._worker.do_later(third_task)
        yield tg.sleep(0.5)
        self.assertEqual(side, [third_task, second_task])

    @tt.gen_test
    def testFlush(self):
        first_task = self._createAsyncMock(2)
        side = []
        second_task = FakeTask(side, 1)
        self._worker.do_later(first_task)
        self._worker.do_later(second_task)

        # wait until first_task is running
        yield tg.sleep(0.5)
        self.assertEqual(len(self._worker._queue._queue), 1)

        # wait until flush task enter the queue
        self._worker.flush(lambda _: _.priority == -2)
        yield tg.sleep(0.5)
        self.assertEqual(len(self._worker._queue._queue), 2)
        self.assertIsNot(self._worker._queue._queue[0].__class__, FakeTask)

        # wait until idle
        yield tg.sleep(1.5)
        self.assertEqual(side, [])

    @tt.gen_test
    def testDoWithException(self):
        def fn():
            raise TestException('magic')

        with self.assertRaises(TestException):
            yield self._worker.do(fn)

    @tt.gen_test
    def testDoLaterWithCallback(self):
        fn = self._createSyncMock()
        cb = utm.MagicMock()
        self._worker.do_later(fn, cb)
        yield tg.sleep(0.5)
        fn.assert_called_once_with()
        cb.assert_called_once_with(42)

    def _createSyncMock(self):
        return utm.Mock(return_value=42)

    def _createAsyncMock(self, delay=None):
        return u.AsyncMock(return_value=42, delay=delay)


class TestTask(ut.TestCase):

    def testID(self):
        a = worker.Task()
        b = worker.Task()
        self.assertLess(a.id_, b.id_)


class FakeTask(worker.Task):

    def __init__(self, side, order):
        super(FakeTask, self).__init__()

        self._side = side
        self._order = order

    def __eq__(self, that):
        return self.priority == that.priority and self._order == that._order

    def __lt__(self, that):
        if self.priority > that.priority:
            return True
        if self.priority < that.priority:
            return False
        return self._order > that._order

    def __call__(self):
        self._side.append(self)
        return self

    def __await__(self):
        yield tg.sleep(0.125)

    @property
    def priority(self):
        return -2


class TestException(Exception):
    pass
