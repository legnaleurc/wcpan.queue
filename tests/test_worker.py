import functools
import unittest as ut
from unittest import mock as utm

from tornado import gen as tg

from tornado_multithread import worker
from . import util as u


class TestAsyncWorker(ut.TestCase):

    def setUp(self):
        self._worker = worker.AsyncWorker()
        self._worker.start()
        self.assertTrue(self._worker.is_alive)

    def tearDown(self):
        u.async_call(self._worker.stop)
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

    def testDoWithSync(self):
        fn = self._createSyncMock()
        rv = u.async_call(self._worker.do, fn)
        fn.assert_called_once_with()
        self.assertEqual(rv, 42)

    def testDoWithAsync(self):
        fn = self._createAsyncMock()
        rv = u.async_call(self._worker.do, fn)
        fn.assert_called_once_with()
        fn.assert_awaited()
        self.assertEqual(rv, 42)

    def testDoLaterWithSync(self):
        fn = self._createSyncMock()
        self._worker.do_later(fn)
        u.async_call(functools.partial(tg.sleep, 0.5))
        fn.assert_called_once_with()

    def testDoLaterWithAsync(self):
        fn = self._createAsyncMock()
        self._worker.do_later(fn)
        u.async_call(functools.partial(tg.sleep, 0.5))
        fn.assert_called_once_with()

    def testDoWithSyncPartial(self):
        fn = self._createSyncMock()
        rv = u.async_call(self._worker.do, functools.partial(fn, 1, k=7))
        fn.assert_called_once_with(1, k=7)
        self.assertEqual(rv, 42)

    def testDoWithAsyncPartial(self):
        fn = self._createAsyncMock()
        rv = u.async_call(self._worker.do, functools.partial(fn, 1, k=7))
        fn.assert_called_once_with(1, k=7)
        fn.assert_awaited()
        self.assertEqual(rv, 42)

    def testDoLaterWithSyncPartial(self):
        fn = self._createSyncMock()
        self._worker.do_later(functools.partial(fn, 1, k=7))
        u.async_call(functools.partial(tg.sleep, 0.5))
        fn.assert_called_once_with(1, k=7)

    def testDoLaterWithAsyncPartial(self):
        fn = self._createAsyncMock()
        self._worker.do_later(functools.partial(fn, 1, k=7))
        u.async_call(functools.partial(tg.sleep, 0.5))
        fn.assert_called_once_with(1, k=7)
        fn.assert_awaited()

    def testRunOrder(self):
        first_task = self._createAsyncMock()
        side = []
        second_task = FakeTask(side, 2)
        third_task = FakeTask(side, 3)
        self._worker.do_later(first_task)
        self._worker.do_later(second_task)
        self._worker.do_later(third_task)
        u.async_call(functools.partial(tg.sleep, 0.5))
        self.assertEqual(side, [third_task, second_task])

    def testFlush(self):
        first_task = self._createAsyncMock()
        side = []
        second_task = FakeTask(side, 1)
        third_task = FlushFakeTask(side, 1)
        self._worker.do_later(first_task)
        self._worker.do_later(second_task)
        self._worker.do_later(third_task)
        u.async_call(functools.partial(tg.sleep, 0.5))
        self.assertEqual(side, [third_task])

    def testDoWithException(self):
        def fn():
            raise TestException('magic')

        with self.assertRaises(TestException):
            u.async_call(self._worker.do, fn)

    def _createSyncMock(self):
        return utm.Mock(return_value=42)

    def _createAsyncMock(self):
        return u.AsyncMock(return_value=42)


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

    def __gt__(self, that):
        if self.priority < that.priority:
            return True
        if self.priority > that.priority:
            return False
        return self._order < that._order

    def __call__(self):
        self._side.append(self)
        return self

    def __await__(self):
        yield tg.sleep(0.125)

    @property
    def priority(self):
        return -2


class FlushFakeTask(FakeTask):

    def __init__(self, side, order):
        super(FlushFakeTask, self).__init__(side, order)

    def __call__(self):
        self._side.append(self)
        raise worker.FlushTasks(self._filter)

    @property
    def priority(self):
        return -1

    def _filter(self, task):
        return task.priority != -2


class TestException(Exception):
    pass