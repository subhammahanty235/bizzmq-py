"""
BizzMQ - A lightweight Redis-based message queue system with Dead Letter Queue support.
"""

__version__ = "1.0.0"

# Import main client class
from .client import BizzMQ

# Import queue-related classes
from .queue import QueueOptions, create_queue

# Import message-related classes
# from .message import MessageOptions

# Define what should be accessible when someone does `from bizzmq import *`
__all__ = ["BizzMQ", "QueueOptions",  "create_queue"]