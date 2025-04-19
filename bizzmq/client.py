"""
Main BizzMQ client module providing the primary interface to the message queue system.
"""

from .redis_client import RedisClient
from .queue import QueueOptions

class BizzMQ:
    def __init__(self, redis_url:str)->None:
        if not redis_url:
            raise ValueError("Redis URL is required")
        
        self.redis = RedisClient(redis_url)
    
    def close(self) -> None:
        if hasattr(self, 'redis') and self.redis:
            self.redis.close()
    
    def create_queue(self, queue_name:str, options:Optional[QueueOptions] = None) -> None:
        from .queue import create_queue
        return create_queue(self.redis, queue_name, options)
    
    