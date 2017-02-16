import functools as ft
import unittest as ut
from unittest import mock as utm
import threading

from tornado import gen as tg, testing as tt

import wcpan.worker as ww
from . import util as u


class TestAsyncWorker(tt.AsyncTestCase):

    def setUp(self):
        super(TestAsyncWorker, self).setUp()
        self._worker = ww.AsyncWorker()
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
        w = ww.AsyncWorker()
        w.start()

        the_loop = w._loop
        w.start()
        w._thread.start.assert_called_once_with()
        self.assertEqual(the_loop, w._loop)

    @utm.patch('tornado.ioloop.IOLoop', autospec=True)
    @utm.patch('threading.Condition', autospec=True)
    @utm.patch('threading.Thread', autospec=True)
    def testStopTwice(self, FakeThread, FakeCondition, FakeIOLoop):
        w = ww.AsyncWorker()
        w.start()
        w.stop()

        w.stop()
        self.assertIsNone(w._thread)
        self.assertIsNone(w._loop)

    @tt.gen_test
    def testDoWithSync(self):
        fn = u.NonBlocker()
        rv = yield self._worker.do(fn)
        self.assertEqual(fn.call_count, 1)
        self.assertEqual(rv, 42)

    @tt.gen_test
    def testDoWithAsync(self):
        fn = u.AsyncNonBlocker()
        rv = yield self._worker.do(fn)
        self.assertEqual(fn.call_count, 1)
        self.assertEqual(rv, 42)

    @tt.gen_test
    def testDoLaterWithSync(self):
        fn = u.NonBlocker()
        rc = u.ResultCollector()
        self._worker.do_later(fn, rc.add)
        self._worker.do_later(rc)
        rc.wait()
        self.assertEqual(fn.call_count, 1)

    @tt.gen_test
    def testDoLaterWithAsync(self):
        fn = u.AsyncNonBlocker()
        rc = u.ResultCollector()
        self._worker.do_later(fn, rc.add)
        self._worker.do_later(rc)
        rc.wait()
        self.assertEqual(fn.call_count, 1)

    @tt.gen_test
    def testDoWithSyncPartial(self):
        fn = u.NonBlocker()
        bound_fn = ft.partial(fn, 1, k=7)
        rv = yield self._worker.do(bound_fn)
        self.assertEqual(fn.called_with, [
            ((1, ), {
                'k': 7,
            }),
        ])
        self.assertEqual(rv, 42)

    @tt.gen_test
    def testDoWithAsyncPartial(self):
        fn = u.AsyncNonBlocker()
        bound_fn = ft.partial(fn, 1, k=7)
        rv = yield self._worker.do(bound_fn)
        self.assertEqual(fn.called_with, [
            ((1, ), {
                'k': 7,
            }),
        ])
        self.assertEqual(rv, 42)

    @tt.gen_test
    def testDoLaterWithSyncPartial(self):
        fn = u.NonBlocker()
        rc = u.ResultCollector()
        bound_fn = ft.partial(fn, 1, k=7)
        self._worker.do_later(bound_fn, rc.add)
        self._worker.do_later(rc)
        rc.wait()
        self.assertEqual(fn.called_with, [
            ((1, ), {
                'k': 7,
            }),
        ])

    @tt.gen_test
    def testDoLaterWithAsyncPartial(self):
        fn = u.AsyncNonBlocker()
        rc = u.ResultCollector()
        bound_fn = ft.partial(fn, 1, k=7)
        self._worker.do_later(bound_fn, rc.add)
        self._worker.do_later(rc)
        rc.wait()
        self.assertEqual(fn.called_with, [
            ((1, ), {
                'k': 7,
            }),
        ])

    @tt.gen_test
    def testRunOrder(self):
        first_task = u.Blocker(rv=1, p=1)
        second_task = u.NonBlocker(rv=2, p=2)
        third_task = u.NonBlocker(rv=3, p=3)

        rc = u.ResultCollector()

        self._worker.do_later(first_task, rc.add)
        first_task.wait_for_enter()
        self._worker.do_later(second_task, rc.add)
        self._worker.do_later(third_task, rc.add)
        self._worker.do_later(rc)
        first_task.continue_for_exit()

        rc.wait()

        self.assertEqual(rc.values, [1, 3, 2])

    @tt.gen_test
    def testFlush(self):
        first_task = u.Blocker(rv=1, p=1)
        second_task = u.NonBlocker(rv=2, p=2)
        third_task = u.NonBlocker(rv=3, p=3)

        rc = u.ResultCollector()

        self._worker.do_later(first_task, rc.add)
        first_task.wait_for_enter()
        self._worker.do_later(second_task, rc.add)
        self._worker.do_later(third_task, rc.add)

        q = self._getInternalQueue()
        self.assertEqual(len(q), 2)

        first_task.continue_for_exit()

        # flush third_task
        yield self._worker.flush(lambda _: _.priority == 3)

        self._worker.do_later(rc)

        rc.wait()

        q = self._getInternalQueue()
        self.assertEqual(len(q), 0)
        self.assertEqual(rc.values, [1, 2])

    @tt.gen_test
    def testFlushLater(self):
        first_task = u.Blocker(rv=1, p=1)
        second_task = u.NonBlocker(rv=2, p=2)
        third_task = u.NonBlocker(rv=3, p=3)

        rc = u.ResultCollector()

        self._worker.do_later(first_task, rc.add)
        first_task.wait_for_enter()
        self._worker.do_later(second_task, rc.add)
        self._worker.do_later(third_task, rc.add)

        q = self._getInternalQueue()
        self.assertEqual(len(q), 2)

        bk = threading.Event()
        def on_flushed(rv):
            bk.set()

        self._worker.flush_later(lambda _: _.priority == 3, on_flushed)

        first_task.continue_for_exit()
        bk.wait()

        self._worker.do_later(rc)
        rc.wait()

        q = self._getInternalQueue()
        self.assertEqual(len(q), 0)
        self.assertEqual(rc.values, [1, 2])

    @tt.gen_test
    def testDoWithException(self):
        def fn():
            raise TestException('magic')

        with self.assertRaises(TestException):
            yield self._worker.do(fn)

    def _getInternalQueue(self):
        return self._worker._get_internal_queue()


class TestTask(ut.TestCase):

    def testID(self):
        a = ww.Task()
        b = ww.Task()
        self.assertLess(a.id_, b.id_)


class TestException(Exception):
    pass
