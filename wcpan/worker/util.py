import asyncio
import functools as ft


def sync(fn):
    @ft.wraps(fn)
    def wrapper(*args, **kwargs):
        future = fn(*args, **kwargs)
        rv = asyncio.run(future)
        return rv
    return wrapper
