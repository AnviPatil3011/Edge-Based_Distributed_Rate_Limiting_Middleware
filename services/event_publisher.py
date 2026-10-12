import json
import logging
import queue
import threading
import time

from config import Config

try:
    import pika
except ImportError:
    pika = None

_logger = logging.getLogger(__name__)
_event_queue = queue.Queue()

# Avoid repeatedly trying RabbitMQ when it is unavailable.
_unavailable_until = 0.0
_state_lock = threading.Lock()


def _get_connection():
    if not pika:
        raise RuntimeError("pika is not installed")

    params = pika.URLParameters(Config.RABBITMQ_URL)
    params.connection_attempts = 1
    params.socket_timeout = 0.3
    params.stack_timeout = 0.5
    return pika.BlockingConnection(params)


def publish_event(event):
    global _unavailable_until

    # If RabbitMQ is unavailable, queue events without reconnecting
    # on every API request.
    with _state_lock:
        if time.monotonic() < _unavailable_until:
            _event_queue.put(event)
            return

    if not pika:
        _event_queue.put(event)
        return

    connection = None
    try:
        connection = _get_connection()
        channel = connection.channel()
        channel.queue_declare(
            queue="rate_limit_events",
            durable=True,
        )

        channel.basic_publish(
            exchange="",
            routing_key="rate_limit_events",
            body=json.dumps(event),
            properties=pika.BasicProperties(delivery_mode=2),
        )

    except Exception as exc:
        _logger.debug("RabbitMQ unavailable; queuing event: %s", exc)
        _event_queue.put(event)

        with _state_lock:
            _unavailable_until = time.monotonic() + 5.0

    finally:
        if connection is not None:
            try:
                if connection.is_open:
                    connection.close()
            except Exception:
                pass