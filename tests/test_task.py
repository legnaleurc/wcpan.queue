import unittest as ut

import wcpan.worker as ww


class TestTask(ut.TestCase):

    def testID(self):
        a = ww.Task()
        b = ww.Task()
        self.assertLess(a.id_, b.id_)
