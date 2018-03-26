wcpan.worker
============

An asynchronous task queue with priority support.

.. code:: python

    from wcpan.worker import AsyncQueue, Task


    class HighPriorityTask(Task):

        @property
        def priority(self) -> int:
            return 2


    class LowPriorityTask(Task):

        @property
        def priority(self) -> int:
            return 1


    # Note this queue is non-preemptive.
    queue = AsyncQueue()
    queue.start()

    # function_2 will come first.
    queue.post(LowPriorityTask(function_1))
    queue.post(HighPriorityTask(function_2))

    # cancel pending tasks
    queue.flush()

    # wait for executing task (if any) ends, then stop the queue
    await queue.stop()
