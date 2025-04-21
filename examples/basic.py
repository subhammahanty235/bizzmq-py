from bizzmq import BizzMQ
from bizzmq.queue import QueueOptions
from bizzmq.message import MessageOptions
import asyncio
import time
import threading
import signal

def message_handler(message):
    print(f"Processing message: {message}")
    print(f"Processed message successfully!")
    return True

async def main():
    client = BizzMQ(redis_url="redis://redis:6379")
    queue_name = "email-queue"
    options = QueueOptions(config_dead_letter_queue=1, max_retries=3)
    client.create_queue(queue_name, options)

    message_data = {
        "id": 1,
        "content": "Hello from BizzMQ!",
        "timestamp": time.time()
    }
    message_data2 = {
        "id": 2,
        "content": "Hello from BizzMQ! Second Message ",
        "timestamp": time.time()
    }

    client.publish_message_to_queue(queue_name, message_data, MessageOptions())
    client.publish_message_to_queue(queue_name, message_data2, MessageOptions())
    print(f"Starting consumer for queue: {queue_name}")


    # Now we have to Create a threading event to block the main thread
    stop_event = threading.Event()
    
    # signal handler for graceful shutdown
    def signal_handler(sig, frame):
        print("\nShutting down...")
        if cleanup:
            cleanup()
        stop_event.set()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    
    # consumer
    cleanup = client.consume_message_from_queue(queue_name, message_handler)
    
    try:
        print("Consumer running. Press Ctrl+C to stop...")
        # Wait indefinitely until cleanup is called, it's a short term approach till i find something with much performance boost
        stop_event.wait()
    except KeyboardInterrupt:
        print("Shutting down...")
        if cleanup:
            cleanup()

if __name__ == "__main__":
    asyncio.run(main())
