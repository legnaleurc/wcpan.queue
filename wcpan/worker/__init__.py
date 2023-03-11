from importlib.metadata import version

from .aioqueue import AioQueue, create_queue, consume_all
from .queue import AsyncQueue
from .task import Task


__version__ = version(__package__ or __name__)
__all__ = ("AsyncQueue", "Task", "AioQueue", "create_queue", "consume_all")
