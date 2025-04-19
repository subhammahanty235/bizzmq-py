import redis
import time
class QueueOptions:
    def __init__(self, config_dead_letter_queue=False,  retry=None, max_retries=3):
        self.config_dead_letter_queue = config_dead_letter_queue
        self.retry = retry
        self.max_retries = max_retries
    
    def to_dict(self):
        options = {
            "config_dead_letter_queue": self.config_dead_letter_queue,
            "max_retries":self.max_retries,
        }

        if self.retry is not None:
            options["retry"] = self.retry
        
        return options

    @classmethod
    def from_dict(cls, options_dict):
        return cls(
            config_dead_letter_queue=options_dict.get("config_dead_letter_queue", False),
            retry=options_dict.get("retry"),
            max_retries=options_dict.get("max_retries", 3)
        )


# Queue related functions - 1. Create a new queue

def create_queue(redis_client : redis.Redis , queue_name: str , queue_options:'QueueOptions') -> None :
    if not queue_name:
        raise ValueError("Queue name is required")
    
    queue_meta_key  = f"queue_meta:{queue_name}"

    exists = redis_client.exists(queue_meta_key)
    if exists:
        print(f"✅ Queue \"{queue_name}\" already exists.")
        return

    queue_data = {
        "createdAt": int(time.time() * 1000)  
    }

    options_dict = queue_options.to_dict()
    for key, value in options_dict.items():
        queue_data[key] = value

    try:
        redis_client.hset(queue_meta_key, mapping=queue_data)
        print(f"📌 Queue \"{queue_name}\" created successfully.")
    
    except Exception as e:
        error_msg = f"Failed to create queue: {str(e)}"
        print(f"❌ {error_msg}")
        raise Exception(error_msg)

