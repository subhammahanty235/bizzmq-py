"""
Redis client for BizzMQ.
Handles connections and basic Redis operations.
"""
import redis
from redis import Redis


class RedisClient:
    def __init__(self, redis_url:str) -> None:
        if not redis_url:
            raise ValueError("Redis URL is required")
        
        try:
            self.client = redis.from_url(redis_url)
            self.client.ping()
            self._print_welcome_message()
        
        except redis.RedisError as err :
            raise ConnectionError(f"Failed to connect to Redis: {str(err)}") from err

    def get_redis_client(self)->Redis:
        return self.client
    
    def close(self)->None:
        if hasattr(self, 'client') and self.client:
            self.client.close()

    def _print_welcome_message(self)->None:
        print("╔════════════════════════════════════════════════════════════╗")
        print("║                                                            ║")
        print("║   🚀 BizzMQ Queue System v1.0.0 (Python Edition)           ║")
        print("║         A lightweight message queue for Python             ║")
        print("║                                                            ║")
        print("╠════════════════════════════════════════════════════════════╣")
        print("║                                                            ║")
        print("║   ⚠️  EARLY VERSION NOTICE                                  ║")
        print("║       This is an early version and may contain bugs.       ║")
        print("║       Please report any issues you encounter to:           ║")
        print("║       github.com/subhammahanty235/bizzmq-py/issues         ║")
        print("║                                                            ║")
        print("║   💡 USAGE                                                 ║")
        print("║       1. Create queues                                     ║")
        print("║       2. Publish messages                                  ║")
        print("║       3. Set up consumers                                  ║")
        print("║                                                            ║")
        print("║   📚 For documentation visit:                              ║")
        print("║       https://bizzmq.vercel.app/docs/py-docs               ║")
        print("║                                                            ║")
        print("╚════════════════════════════════════════════════════════════╝")
        print("🔌 Connected to Redis Database")


