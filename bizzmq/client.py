"""
Main BizzMQ client module providing the primary interface to the message queue system.
"""

from .redis_client import RedisClient
from .queue import QueueOptions
from .message import MessageOptions
from typing import Optional, Any, Dict, Union, Callable, Tuple

class BizzMQ:
    def __init__(self, redis_url:str)->None:
        if not redis_url:
            raise ValueError("Redis URL is required")
        
        self.redis = RedisClient(redis_url)
        self.redisInstance = self.redis.get_redis_client()
    
    def close(self) -> None:
        if hasattr(self, 'redis') and self.redis:
            self.redis.close()
    
    def create_queue(self, queue_name:str, options:Optional[QueueOptions] = None) -> None:
        from .queue import create_queue
        return create_queue(self.redisInstance, queue_name, options)

    def publish_message_to_queue(self, queue_name:str, message:Any, message_options:Optional[MessageOptions] = None) -> None:
        from .producer import publish_message_to_queue
        return publish_message_to_queue(self.redisInstance, queue_name, message, message_options)
    
    
