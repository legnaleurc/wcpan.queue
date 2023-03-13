from importlib.metadata import version

from .queue import AioQueue


__version__ = version(__package__ or __name__)
__all__ = ("AioQueue",)
