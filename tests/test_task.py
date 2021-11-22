import unittest

from wcpan.worker import Task


class TestTask(unittest.TestCase):

    def testID(self):
        a = Task()
        b = Task()
        self.assertLess(a.id_, b.id_)
