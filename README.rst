wcpan.worker
============

A multithread worker for Tornado.

.. code:: python

    from wcpan.worker import AsyncWorker

    worker = AsyncWorker()
    worker.start()

    # this will be run on another thread
    result = await worker.do(your_blocking_function)

    # do this later
    worker.do_later(your_blocking_function)
