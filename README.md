# wcpan.queue

An utility for `asyncio.Queue`.

```python
from wcpan.queue import AioQueue


async def task(): ...


async def amain():
    # Creates a priority queue.
    # Use AioQueue.fifo() for FIFO and AioQueue.lifo() for LIFO.
    with AioQueue.priority() as queue:
        # Push a task which priority is 1, lesser number has higher priority.
        # Default is 0.
        # Priority is ignored for FIFO and LIFO queues.
        await queue.push(task(), 1)
        # Spawns 8 consumers to consume the queue.
        # Default is 1.
        await queue.consume(8)
```
