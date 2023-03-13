from asyncio import CancelledError, create_task, sleep
from unittest import IsolatedAsyncioTestCase

from wcpan.worker import AioQueue


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
    await q.push(walk_tree(rv, q, n.l))
    await q.push(walk_tree(rv, q, n.r))


async def long_task(rv: list[int]):
    try:
        rv.append(0)
        await sleep(60)
        rv.append(1)
    except CancelledError:
        rv.append(2)
        raise


async def plain_task(rv: list[str], value: str):
    rv.append(value)


async def bad_task():
    raise RuntimeError("I AM ERROR")


async def nop():
    pass


class AioQueueTestCase(IsolatedAsyncioTestCase):
    async def test_bad_queue_size(self):
        q = AioQueue.fifo()
        with self.assertRaises(ValueError):
            await q.consume()

    async def test_bad_consumer_size(self):
        q = AioQueue.fifo()
        await q.push(nop())
        with self.assertRaises(ValueError):
            await q.consume(-1)
        q.purge()

    async def test_exception(self):
        q = AioQueue.fifo()
        await q.push(bad_task())
        with self.assertRaises(ExceptionGroup) as e:
            await q.consume()
        self.assertEqual(q.size, 0)

    async def test_recursive(self):
        tree = create_tree()
        q = AioQueue.fifo()
        rv: list[int] = []
        await q.push(walk_tree(rv, q, tree))
        await q.consume()
        self.assertEqual(rv, [0, 1, 2, 3, 4, 5, 6])

    async def test_cancel(self):
        q = AioQueue.fifo()
        rv: list[int] = []
        await q.push(long_task(rv))
        task = create_task(q.consume())
        await sleep(0)
        task.cancel()
        try:
            await task
        except CancelledError:
            pass
        self.assertEqual(rv, [0, 2])
        self.assertEqual(q.size, 0)

    async def test_purge(self):
        q = AioQueue.fifo()
        await q.push(nop())
        q.purge()
        self.assertEqual(q.size, 0)

    async def test_priority(self):
        q = AioQueue.priority()
        rv: list[str] = []
        await q.push(plain_task(rv, "a"), 0)
        await q.push(plain_task(rv, "b"), 2)
        await q.push(plain_task(rv, "c"), 1)
        await q.push(plain_task(rv, "d"), 0)
        await q.consume()
        self.assertEqual(rv, ["a", "d", "c", "b"])

    async def test_lifo(self):
        q = AioQueue.lifo()
        rv: list[str] = []
        await q.push(plain_task(rv, "a"))
        await q.push(plain_task(rv, "b"))
        await q.push(plain_task(rv, "c"))
        await q.push(plain_task(rv, "d"))
        await q.consume()
        self.assertEqual(rv, ["d", "c", "b", "a"])
