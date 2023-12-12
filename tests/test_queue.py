from asyncio import CancelledError, create_task, sleep
from typing import Any
from unittest import IsolatedAsyncioTestCase

from wcpan.queue import AioQueue


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


async def void_walk_tree(rv: list[int], q: AioQueue[Any], n: Node | None) -> None:
    if not n:
        return
    rv.append(n.v)
    await q.push(void_walk_tree(rv, q, n.l))
    await q.push(void_walk_tree(rv, q, n.r))


async def walk_tree(q: AioQueue[int], n: Node) -> int:
    if n.l:
        await q.push(walk_tree(q, n.l))
    if n.r:
        await q.push(walk_tree(q, n.r))
    return n.v


async def long_task(rv: list[int]):
    try:
        rv.append(0)
        await sleep(60)
        rv.append(1)
    except CancelledError:
        rv.append(2)
        raise


async def void_task(rv: list[str], value: str):
    rv.append(value)


async def return_task(value: str):
    return value


async def bad_task():
    raise RuntimeError("I AM ERROR")


async def nop():
    pass


class AioQueueTestCase(IsolatedAsyncioTestCase):
    async def test_bad_queue_size(self):
        q = AioQueue[None].fifo()
        with self.assertRaises(ValueError):
            await q.consume()
        with self.assertRaises(ValueError):
            async for _ in q.collect():
                pass

    async def test_bad_consumer_size(self):
        q = AioQueue[None].fifo()
        await q.push(nop())
        with self.assertRaises(ValueError):
            await q.consume(-1)
        q.purge()

    async def test_bad_collector_size(self):
        q = AioQueue[None].fifo()
        await q.push(nop())
        with self.assertRaises(ValueError):
            async for _ in q.collect(-1):
                pass
        q.purge()

    async def test_consume_exception(self):
        q = AioQueue[None].fifo()
        await q.push(bad_task())
        with self.assertRaises(ExceptionGroup):
            await q.consume()
        self.assertEqual(q.size, 0)

    async def test_collect_exception(self):
        q = AioQueue[None].fifo()
        await q.push(bad_task())
        with self.assertRaises(ExceptionGroup):
            async for _ in q.collect():
                pass

    async def test_consume_recursive(self):
        tree = create_tree()
        q = AioQueue[None].fifo()
        rv: list[int] = []
        await q.push(void_walk_tree(rv, q, tree))
        await q.consume()
        self.assertEqual(rv, [0, 1, 2, 3, 4, 5, 6])

    async def test_collect_recursive(self):
        tree = create_tree()
        q = AioQueue[int].fifo()
        await q.push(walk_tree(q, tree))
        rv = [_ async for _ in q.collect()]
        self.assertEqual(rv, [0, 1, 2, 3, 4, 5, 6])

    async def test_consume_cancel(self):
        q = AioQueue[None].fifo()
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

    async def test_collect_cancel(self):
        q = AioQueue[None].fifo()
        rv: list[int] = []
        await q.push(long_task(rv))

        async def to_coro():
            async for _ in q.collect():
                pass

        task = create_task(to_coro())
        await sleep(0)
        task.cancel()
        try:
            await task
        except CancelledError:
            pass
        self.assertEqual(rv, [0, 2])
        self.assertEqual(q.size, 0)

    async def test_purge(self):
        q = AioQueue[None].fifo()
        await q.push(nop())
        q.purge()
        self.assertEqual(q.size, 0)

    async def test_consume_priority(self):
        q = AioQueue[None].priority()
        rv: list[str] = []
        await q.push(void_task(rv, "a"), 0)
        await q.push(void_task(rv, "b"), 2)
        await q.push(void_task(rv, "c"), 1)
        await q.push(void_task(rv, "d"), 0)
        await q.consume()
        self.assertEqual(rv, ["a", "d", "c", "b"])

    async def test_collect_priority(self):
        q = AioQueue[str].priority()
        await q.push(return_task("a"), 0)
        await q.push(return_task("b"), 2)
        await q.push(return_task("c"), 1)
        await q.push(return_task("d"), 0)
        rv = [_ async for _ in q.collect()]
        self.assertEqual(rv, ["a", "d", "c", "b"])

    async def test_consume_lifo(self):
        q = AioQueue[None].lifo()
        rv: list[str] = []
        await q.push(void_task(rv, "a"))
        await q.push(void_task(rv, "b"))
        await q.push(void_task(rv, "c"))
        await q.push(void_task(rv, "d"))
        await q.consume()
        self.assertEqual(rv, ["d", "c", "b", "a"])

    async def test_collect_lifo(self):
        q = AioQueue[str].lifo()
        await q.push(return_task("a"))
        await q.push(return_task("b"))
        await q.push(return_task("c"))
        await q.push(return_task("d"))
        rv = [_ async for _ in q.collect()]
        self.assertEqual(rv, ["d", "c", "b", "a"])

    async def test_consume_many(self):
        q = AioQueue[None].fifo()
        rv: list[str] = []
        await q.push(void_task(rv, "a"))
        await q.push(void_task(rv, "b"))
        await q.push(void_task(rv, "c"))
        await q.push(void_task(rv, "d"))
        await q.consume(4)
        self.assertEqual({"a", "b", "c", "d"}, set(rv))

    async def test_collect_many(self):
        q = AioQueue[str].fifo()
        await q.push(return_task("a"))
        await q.push(return_task("b"))
        await q.push(return_task("c"))
        await q.push(return_task("d"))
        rv = [_ async for _ in q.collect(4)]
        self.assertEqual({"a", "b", "c", "d"}, set(rv))
