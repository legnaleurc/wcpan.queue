import unittest as ut

import wcpan.worker as ww
from wcpan.worker.task import TerminalTask, FlushTask


class TestTask(ut.TestCase):

    def testID(self):
        a = ww.Task()
        b = ww.Task()
        self.assertLess(a.id_, b.id_)

    def testOrder(self):
        a = ww.Task()
        b = TerminalTask()
        c = FlushTask(lambda _: _)
        rv = sorted([a, b, c])
        self.assertEqual(rv, [b, c, a])
