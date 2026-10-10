import json
import logging
import queue
import threading
from config import Config

_logger = logging.getLogger(__name__)

# Optional pika import – if unavailable we operate in fallback mode.
try:
    import pika
except ImportError:  # pragma: no cover
    pika = None
    _logger.debug('pika library not available; RabbitMQ publishing disabled')

# In‑memory queue for events when RabbitMQ cannot be used.
_event_queue = queue.Queue()

def _get_connection():
    """Create a new pika BlockingConnection using the configured RabbitMQ URL."""
    if not pika:
        raise RuntimeError('pika library not available')
    params = pika.URLParameters(Config.RABBITMQ_URL)
    return pika.BlockingConnection(params)

def publish_event(event: dict) -> None:
    """Publish a rate‑limit decision event.

    Attempts to send the event to RabbitMQ. If RabbitMQ is unavailable or the
    publish fails, the event is queued in an in‑memory fallback queue which is
    processed by the background ``SyncWorker``.
    """
    if pika:
        try:
            connection = _get_connection()
            channel = connection.channel()
            channel.queue_declare(queue='rate_limit_events', durable=True)
            body = json.dumps(event)
            properties = pika.BasicProperties(delivery_mode=2)  # persistent
            channel.basic_publish(
                exchange='',
                routing_key='rate_limit_events',
                body=body,
                properties=properties,
            )
            connection.close()
            return
        except Exception as exc:
            _logger.debug("Failed to publish event via RabbitMQ: %s", exc)
    # Fallback – enqueue for background processing.
    _event_queue.put(event)

# NOTE: The synchronization worker is launched explicitly via the entry point
# ``python -m workers.sync_worker``. Importing this module does NOT start any
# background threads; the fallback queue is processed only by the worker.
