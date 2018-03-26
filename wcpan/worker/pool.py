import asyncio
import concurrent.futures as cf
import multiprocessing as mp
import functools as ft


def create_thread_pool():
    return cf.ThreadPoolExecutor(max_workers=mp.cpu_count())


def off_main_thread_method(pool_attr: str):
    def off_main_thread(fn):
        @ft.wraps(fn)
        def wrapper(self, *args, **kwargs):
            loop = asyncio.get_event_loop()
            pool = getattr(self, pool_attr)
            bound_fn = ft.partial(fn, self, *args, **kwargs)
            future = loop.run_in_executor(pool, bound_fn)
            return future
        return wrapper
    return off_main_thread
