from typing import Any, Dict, Optional, Union
from dataclasses import dataclass, field
@dataclass
class MessageOptions:
    priority: int  = 0
    retries: int = 1

class Message:
    def __init__(self, queue_name: str, message_id: str, message: Any, options:MessageOptions):
        self.queue_name = queue_name
        self.message_id = message_id
        self.message = message
        self.options = options
        self.timestamp_created = int(time.time() * 1000)
        self.timestamp_updated = self.timestamp_created
        self.status = "waiting"
    
    def to_json(self) -> Dict[str, Any]:
        return {
            "queue_name": self.queue_name,
            "message_id": self.message_id,
            "message": self.message,
            "options": {
                "priority": self.options.priority,
                "retries": self.options.retries
            },
            "timestamp_created": self.timestamp_created,
            "timestamp_updated": self.timestamp_updated,
            "status": self.status
        }
    
def new_message( queue_name: str, message_id: str, message: Any, options: MessageOptions) -> Message:
    return Message(queue_name, message_id, message, options)
