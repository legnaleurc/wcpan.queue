import asyncio
import functools as ft


def sync(fn):
    @ft.wraps(fn)
    def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        future = fn(*args, **kwargs)
        rv = loop.run_until_complete(future)
        return rv
    return wrapper
