from bizzmq import BizzMQ
from bizzmq.queue import QueueOptions
from bizzmq.message import MessageOptions
import asyncio
import time

async def main():
    client = BizzMQ(redis_url="redis://redis:6379")
    queue_name = "test-queue-000"
    options = QueueOptions(config_dead_letter_queue=1, max_retries=3)
    client.create_queue(queue_name, options)

    message_data = {
        "id": 1,
        "content": "Hello from BizzMQ!",
        "timestamp": time.time()
    }

    client.publish_message_to_queue(queue_name, message_data, MessageOptions())

if __name__ == "__main__":
    asyncio.run(main())




"""
docker compose down
docker compose build
docker compose up -d

docker compose exec bizzmq-python bash

python examples/basic.py
"""