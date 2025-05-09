# BizzMQ-Py

A lightweight Redis-based message queue system with Dead Letter Queue support, implemented in Python.

## Features

- **Redis-Based Queueing**: Leverages Redis for message storage and delivery
- **Retry Mechanism**: Automatically retries failed jobs based on configured attempts
- **Message Persistence**: Messages remain in Redis until acknowledged
- **Graceful Shutdown**: Handles system signals and shuts down consumers cleanly
- **Clean Pythonic API**: Easy-to-use, async-compatible design

## 🧱 Installation

```bash
pip install bizzmq
```

## Prerequisites

- Python 3.8 or higher
- Redis server (v5 or higher)

## Quick Start

### Basic Usage

```Python
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
    client = BizzMQ(redis_url="redis://localhost:6379")
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
        "content": "Hello from BizzMQ! Second Message",
        "timestamp": time.time()
    }

    client.publish_message_to_queue(queue_name, message_data, MessageOptions())
    client.publish_message_to_queue(queue_name, message_data2, MessageOptions())
    print(f"Starting consumer for queue: {queue_name}")

    stop_event = threading.Event()

    def signal_handler(sig, frame):
        print("\nShutting down...")
        if cleanup:
            cleanup()
        stop_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    cleanup = client.consume_message_from_queue(queue_name, message_handler)

    try:
        print("Consumer running. Press Ctrl+C to stop...")
        stop_event.wait()
    except KeyboardInterrupt:
        print("Shutting down...")
        if cleanup:
            cleanup()

if __name__ == "__main__":
    asyncio.run(main())

```

### Using Dead Letter Queue with Retries

```Python
from bizzmq import BizzMQ
from bizzmq.queue import QueueOptions
from bizzmq.message import MessageOptions
import time
import threading
import signal

def intentional_failure_handler(message):
    print(f"Receiving message: {message}")
    # Simulate a failure
    raise Exception("Simulated failure to demonstrate DLQ")

def dlq_message_handler(message):
    print(f"DLQ received failed message: {message}")
    # Here you would implement your DLQ handling logic
    return True

def main():
    client = BizzMQ(redis_url="redis://redis:6379")
    
    # Create main queue with DLQ support
    queue_name = "main-queue"
    options = QueueOptions(config_dead_letter_queue=True, max_retries=2)
    client.create_queue(queue_name, options)
    
    # DLQ name is automatically set to queue_name + "_dlq"
    dlq_name = f"{queue_name}_dlq"
    dlq_options = QueueOptions()
    client.create_queue(dlq_name, dlq_options)
    
    message_data = {
        "id": 1,
        "content": "This message will fail and go to DLQ",
        "timestamp": time.time()
    }
    
    # Publish a message that will fail
    client.publish_message_to_queue(queue_name, message_data, MessageOptions())
    
    # Setup signal handler for graceful shutdown
    stop_event = threading.Event()
    
    def signal_handler(sig, frame):
        print("\nShutting down...")
        if main_cleanup:
            main_cleanup()
        if dlq_cleanup:
            dlq_cleanup()
        stop_event.set()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start consumers
    main_cleanup = client.consume_message_from_queue(queue_name, intentional_failure_handler)
    dlq_cleanup = client.consume_message_from_queue(dlq_name, dlq_message_handler)
    
    try:
        print("Consumers running. Press Ctrl+C to stop...")
        stop_event.wait()
    except KeyboardInterrupt:
        print("Shutting down...")
        if main_cleanup:
            main_cleanup()
        if dlq_cleanup:
            dlq_cleanup()

if __name__ == "__main__":
    main()
```

## API Reference

### Constructor

#### `BizzMQ(redis_url: str)`

Creates a new BizzMQ instance connected to the specified Redis server.

- `redis_url` (string): Redis connection string (e.g., "redis://localhost:6379")
- Returns: A BizzMQ instance and any error that occurred during initialization

### Queue Management

#### `create_queue(queue_name: str, options: QueueOptions)`

Creates a new queue. If the queue already exists, this operation is skipped.

- `queue_name` (str): Name of the queue to create
- `options` (QueueOptions): Queue configuration options
  - `config_dead_letter_queue` (bool): Whether to create a DLQ for this queue
  - `max_retries` (int): Maximum number of retry attempts before sending to DLQ
  - `retry` (int): Initial retry count for messages in this queue

#### `publish_message_to_queue(queue_name: str, message: dict, options: MessageOptions)`

Publishes a message to the specified queue.

- `queue_name` (string): Name of the queue
- `message`: The message/job data to be processed
- `options` (MessageOptions): Optional message-specific settings
  - `priority` (int64): Message priority level
  - `retries` (int64): Custom retry setting for this message

Returns the generated message ID and any error that occurred.

#### `consume_message_from_queue(queue_name: str, handler: Callable) -> Callable`

Starts consuming messages from the specified queue.

- `queue_name` (string): Name of the queue to consume from
- `handler` function: Function to process each message
  - Should return an error if processing fails

Returns a cleanup function that should be called to stop consuming, and any error that occurred.


## Error Handling and Retry Flow

When a job processing fails (handler returns an error):

1. The error is caught and logged
2. If retry is enabled and max_retries > 0:
   - The job retry count is incremented
   - The job is added back to the queue
3. If retry count exceeds maxRetries or retries are disabled:
   - The job is moved to the Dead Letter Queue (if enabled)
   - Error details are preserved with the job for debugging

## Best Practices

1. **Always enable Dead Letter Queues** for production workloads to capture failed jobs
2. **Use appropriate contexts** for proper cancellation and timeouts
3. **Implement graceful shutdown** by calling the cleanup function returned by `consume_message_from_queue`
4. **Set appropriate MaxRetries** based on the nature of expected failures
5. **Include relevant metadata** in your job data for easier debugging
6. **Check your DLQ regularly** for repeated failures that might indicate systemic issues
7. **Always use `defer mq.Close()`** to ensure Redis connections are properly closed

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
