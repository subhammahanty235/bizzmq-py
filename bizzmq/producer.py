import redis
import time
from .message import Message
from typing import Any, Dict, Optional, Union


def publish_message_to_queue(self, redis_client: redis.Redis , queue_name:str , message:Any , message_options: "MessageOptions") -> str:
    if not queue_name:
        raise ValueError("❌ Queue name not provided")
    
    queue_meta_key = f"queue_meta:{queue_name}"
    queue_key = f"queue:{queue_name}"
    redis_client_instance = self.redis.get_redis_client()

    exists = redis_client_instance.exists(queue_meta_key)

    if not exists:
        raise ValueError(f"❌ Queue \"{queue_name}\" does not exist. Create it first")
    
    message_id = f"message:{int(time.time() * 1000)}"
    message_obj = Message(queue_name, message_id, message, options)

    try:
        message_json = json.dumps(message_obj.to_json())
    except (TypeError, ValueError) as e:
        raise RuntimeError(f"Failed to marshal message: {str(e)}")
    
    try:
        redis_client.lpush(queue_key, message_json)
    except Exception as e:
        raise RuntimeError(f"Failed to push message to queue: {str(e)}")

    print(f"📩 Job added to queue \"{queue_name}\" - ID: {message_id}")
    return message_id

