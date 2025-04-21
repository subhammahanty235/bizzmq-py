import redis
import time
from .message import Message, update_lifecycle_status
from .producer import publish_message_to_queue
from typing import Any, Dict, Optional, Callable, Tuple
import json
import threading
import traceback

def consume_message_from_queue(redis_client: redis.Redis, queue_name: str, callback:Callable[[Dict[str, Any]], None]) -> Tuple[Callable, Optional[Exception]]:
    if not queue_name:
        return None, Exception("❌ Queue name not provided")
    
    queue_key = f"queue:{queue_name}"
    queue_meta_key = f"queue_meta:{queue_name}"

    # Redis client for pub sub operations
    subscriber = redis_client.client()

    try:
        # Fetching the queue options, it's important to check differnet configs like dlq etc
        queue_options_map = redis_client.hgetall(queue_meta_key)
    except Exception as e:
        return None, Exception(f"Failed to get queue options: {str(e)}")
    
    use_dead_letter_queue = queue_options_map.get("config_dead_letter_queue") == "1" or  queue_options_map.get("config_dead_letter_queue") == 1
    max_retries = 3 

    if "maxRetries" in queue_options_map:
        try:
            # Dynamically set the maxRetries in case it's configured in queue options, else default value 3
            max_retries = int(queue_options_map["maxRetries"])
        except ValueError:
            pass

    # flag to control consumer threads
    should_stop = threading.Event()

    # function to process the jobs/messages using the message string
    def process_job(message_str: str) -> Optional[Exception]:
        if not message_str:
            return None
        
        try:
            message_obj = json.loads(message_str)
            # LifeCycle Update
            message_obj = update_lifecycle_status(message_obj, "processing")

            if isinstance(message_obj.get("message"), dict):
                message_data = message_obj["message"]
            else:
                # If it's not a dict, put it in a data field
                message_data = {"data": message_obj.get("message")}

            try:
                callback(message_data)
                # LifeCycle Update
                message_obj = update_lifecycle_status(message_obj, "processed")
                return None
            except Exception as e :
                # LifeCycle Update
                message_obj = update_lifecycle_status(message_obj, "failed")
                if use_dead_letter_queue:
                    if max_retries > 0:
                        self._requeue_message(ctx, queuename, message_obj, err)
                    else:
                        self._move_message_to_dlq(ctx, queuename, message_obj, err)
                else:
                    print("⚠️ Message failed but no DLQ configured")
                    
                return err

        except json.JSONDecodeError as e:
            print(f"❌ Failed to parse message: {str(e)}")
            return e

        return None
    
    """ Function to Process any existing jobs in the queue"""
    def process_existing_jobs():
        
        while True:
            try:
                message = redis_client.rpop(queue_key)
                if not message:
                    break
                        
                err = process_job(message.decode('utf-8') if isinstance(message, bytes) else message)
                if err:
                    print(f"❌ Error processing message: {str(err)}")
            except Exception as e:
                print(f"❌ Error popping message from queue: {str(e)}")
                break

    process_existing_jobs()
    # Subscribe to the queue for real-time notifications
    pubsub = subscriber.pubsub()
    pubsub.subscribe(queue_key)
    def subscriber_thread():
        for message in pubsub.listen():
            if should_stop.is_set():
                break
                
            if message["type"] == "message":
                print(f"🔔 New job notification received: {message['data']}")
                
                # Pop a message from the queue and process it
                try:
                    message = redis_client.rpop(queue_key)
                    if message:
                        message_str = message.decode('utf-8') if isinstance(message, bytes) else message
                        err = process_job(message_str)
                        if err:
                            print(f"❌ Error processing message: {str(err)}")
                except Exception as e:
                    print(f"❌ Error popping message after notification: {str(e)}")
        
    thread = threading.Thread(target=subscriber_thread)
    thread.daemon = True
    thread.start()

    # Start fallback ticker thread to check for missed messages
    def fallback_thread():
        while not should_stop.is_set():
            try:
                message = redis_client.rpop(queue_key)
                if message:
                    print("⚠️ Fallback found unprocessed job")
                    message_str = message.decode('utf-8') if isinstance(message, bytes) else message
                    err = process_job(message_str)
                    if err:
                        print(f"❌ Error processing message from fallback: {str(err)}")
                    
                    # Process any other messages that might be in the queue
                    process_existing_jobs()
            except Exception as e:
                print(f"❌ Error in fallback check: {str(e)}")
                
            # Sleep for fallback interval
            time.sleep(5)  # 5 second interval

    fallback = threading.Thread(target=fallback_thread)
    fallback.daemon = True
    fallback.start()

    def cleanup():
        should_stop.set()
        pubsub.unsubscribe()
        pubsub.close()
            
    return cleanup, None


# Function to requeue a message
def _requeue_message(redis_client: redis.Redis, queuename: str, message: dict, processing_err: Exception) -> Optional[Exception]:
    queue_meta_key = f"queue_meta:{queuename}"

    try:
        queue_options_map = redis_client.hgetall(queue_meta_key)
    except Exception as e:
        return Exception(f"Failed to get queue options: {str(e)}")
    

    
    max_retries = 3 
    if "maxRetries" in queue_options_map:
        try:
            max_retries = int(queue_options_map["maxRetries"])
        except (ValueError, TypeError):
            pass

    
    retry_count = 0
    if "options" in message and isinstance(message["options"], dict):
        retry_count = message["options"].get("retryCount", 0)
    
    # Now the retry count needs to be updated
    retry_count += 1
    if retry_count <= max_retries:
        if "options" not in message or not isinstance(message["options"], dict):
            message["options"] = {}
    
        message["options"]["retryCount"] = retry_count
        message["options"]["timestamp_updated"] = int(time.time() * 1000)
        message = update_lifecycle_status(message, "requeued")

        message_json = json.dumps(message)
        queue_key = f"queue:{queuename}"
        redis_client.lpush(queue_key, message_json)

        print(f"🔄 Message Requeued for retry (attempt {retry_count}/{max_retries})")
        return None
    
    else:
        return _move_message_to_dlq(redis_client, queuename, message, processing_err)


def _move_message_to_dlq(redis_client: redis.Redis, queuename: str, message: dict, processing_err: Exception) -> Optional[Exception]:
    queue_meta_key = f"queue_meta:{queuename}"
    try:
        queue_options_map = redis_client.hgetall(queue_meta_key)
    except Exception as e:
        return Exception(f"Failed to get queue options: {str(e)}")

    dead_letter_queue_value = False
    dlq_option = queue_options_map.get("config_dead_letter_queue")
    if dlq_option:
        try:
            dead_letter_queue_value = dlq_option in ("1", "true", "True")
        except (ValueError, TypeError):
            pass
                
    if not dead_letter_queue_value:
            print(f"No Dead Letter Queue configured for this Queue, Failed message discarded")
            return None

    dlq_name = f"{queuename}_dlq"
    error_message = str(processing_err) if processing_err else "Unknown error"
    if "options" not in message or not isinstance(message["options"], dict):
        message["options"] = {}
    message["options"].update({
        "message": error_message,
        "stack": traceback.format_exc(),
        "timestamp": int(time.time() * 1000),
        "originalQueue": queuename,
        "failedAt": int(time.time() * 1000)
    })

    message_data = message.get("message", {"error": "No data found in original message"})
    options = message.get("options", {})
    try:
        publish_message_to_queue(redis_client, dlq_name, message_data, options)
        print(f"⚠️ Message moved to Dead Letter Queue {dlq_name}")
        return None
    except Exception as e:
        return Exception(f"Failed to publish message to DLQ: {str(e)}")
