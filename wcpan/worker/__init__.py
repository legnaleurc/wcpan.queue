from importlib.metadata import version

from .queue import AsyncQueue
from .task import Task


__version__ = version(__package__ or __name__)
