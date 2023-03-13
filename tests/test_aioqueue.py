from asyncio import CancelledError, create_task, sleep
from unittest import IsolatedAsyncioTestCase

from wcpan.worker import AioQueue, consume_all, create_queue, purge_queue


class Node:
    def __init__(self, v: int) -> None:
        self.v = v
        self.l: Node | None = None
        self.r: Node | None = None


def create_tree() -> Node:
    r = Node(0)
    r.l = Node(1)
    r.r = Node(2)
    r.l.l = Node(3)
    r.l.r = Node(4)
    r.r.l = Node(5)
    r.r.r = Node(6)
    return r


async def walk_tree(rv: list[int], q: AioQueue, n: Node | None):
    if not n:
        return
    rv.append(n.v)
    await q.put(walk_tree(rv, q, n.l))
    await q.put(walk_tree(rv, q, n.r))


async def long_task(rv: list[int]):
    try:
        rv.append(0)
        await sleep(60)
        rv.append(1)
    except CancelledError:
        rv.append(2)
        raise


async def bad_task():
    raise RuntimeError("I AM ERROR")


async def nop():
    pass


class AioQueueTestCase(IsolatedAsyncioTestCase):
    async def test_bad_queue_size(self):
        q = create_queue()
        with self.assertRaises(ValueError):
            await consume_all(q)

    async def test_bad_consumer_size(self):
        q = create_queue()
        await q.put(nop())
        with self.assertRaises(ValueError):
            await consume_all(q, -1)
        purge_queue(q)

    async def test_exception(self):
        q = create_queue()
        await q.put(bad_task())
        with self.assertRaises(ExceptionGroup) as e:
            await consume_all(q)
        self.assertEqual(q.qsize(), 0)

    async def test_recursive(self):
        tree = create_tree()
        q = create_queue()
        rv: list[int] = []
        await q.put(walk_tree(rv, q, tree))
        await consume_all(q)
        self.assertEqual(rv, [0, 1, 2, 3, 4, 5, 6])

    async def test_cancel(self):
        q = create_queue()
        rv: list[int] = []
        await q.put(long_task(rv))
        task = create_task(consume_all(q))
        await sleep(0)
        task.cancel()
        try:
            await task
        except CancelledError:
            pass
        self.assertEqual(rv, [0, 2])
        self.assertEqual(q.qsize(), 0)

    async def test_purge(self):
        q = create_queue()
        await q.put(nop())
        purge_queue(q)
        self.assertEqual(q.qsize(), 0)
