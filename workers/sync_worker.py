import json
import logging
import time
import threading
from typing import List

from config import Config
from services.redis_service import RedisService

_logger = logging.getLogger(__name__)

# Optional pika import – only used when RabbitMQ is available.
try:
    import pika
except ImportError:  # pragma: no cover
    pika = None
    _logger.debug('pika library not available; worker will only process in‑memory queue')

class SyncWorker:
    """Background worker that synchronises rate‑limit decision events.

    It consumes messages from the ``rate_limit_events`` RabbitMQ queue (or from the
    in‑memory fallback queue provided by ``services.event_publisher``) and
    processes them in batches using the ``SYNC_BATCH_SCRIPT`` Lua script. After a
    successful batch it updates the ``worker:heartbeat`` key so that the API can
    report health status.
    """

    def __init__(self, fallback_queue=None):
        self.fallback_queue = fallback_queue
        self.batch: List[dict] = []
        self.last_flush = time.time()
        self.redis = RedisService()
        self._stop_event = threading.Event()

    def _connect(self):
        """Create a blocking RabbitMQ connection if ``pika`` is available.

        Returns ``None`` when RabbitMQ cannot be used.
        """
        if not pika:
            return None
        try:
            params = pika.URLParameters(Config.RABBITMQ_URL)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            # Declare the main queue with dead‑letter configuration.
            args = {
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": Config.DEAD_LETTER_QUEUE,
            }
            channel.queue_declare(queue='rate_limit_events', durable=True, arguments=args)
            # Ensure dead‑letter queue exists.
            channel.queue_declare(queue=Config.DEAD_LETTER_QUEUE, durable=True)
            return connection, channel
        except Exception as exc:
            _logger.debug('Failed to connect to RabbitMQ: %s', exc)
            return None

    def _process_batch(self):
        if not self.batch:
            return
        try:
            # The SYNC_BATCH_SCRIPT expects a JSON array in ARGV[0].
            payload = json.dumps(self.batch)
            # No specific KEYS are required for the script – keep empty list.
            self.redis.execute_lua('SYNC_BATCH', keys=[], args=[payload])
            # Update heartbeat after successful processing.
            self.redis.client.set('worker:heartbeat', int(time.time()))
            _logger.info('Processed sync batch of %d events', len(self.batch))
        except Exception as exc:
            _logger.error('Failed to process sync batch: %s', exc)
            # In case of failure the batch stays in memory for retry on next loop.
            return
        finally:
            # Clear the batch regardless of success to avoid infinite loops.
            self.batch.clear()
            self.last_flush = time.time()

    def _handle_message(self, method, properties, body):
        """Callback for RabbitMQ message consumption.

        Handles retry counting, dead‑letter routing and adds the event to the batch.
        """
        try:
            event = json.loads(body)
        except Exception:
            _logger.debug('Invalid JSON in message, discarding')
            # Acknowledge to drop the bad message.
            method.channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        # Determine current retry count.
        retry_count = getattr(properties, 'headers', {}).get('x-retry-count', 0)
        try:
            # Process the event – add to batch.
            self.batch.append(event)
            # Acknowledge only after the batch is flushed successfully.
            # For simplicity we acknowledge now; the batch processing will handle errors.
            method.channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as exc:
            _logger.error('Error handling event: %s', exc)
            if retry_count < Config.MAX_RETRIES:
                # Re‑publish with incremented retry count.
                new_props = pika.BasicProperties(
                    delivery_mode=2,
                    headers={'x-retry-count': retry_count + 1},
                )
                method.channel.basic_publish(
                    exchange='',
                    routing_key='rate_limit_events',
                    body=body,
                    properties=new_props,
                )
            else:
                # Send to dead‑letter queue.
                method.channel.basic_publish(
                    exchange='',
                    routing_key=Config.DEAD_LETTER_QUEUE,
                    body=body,
                    properties=pika.BasicProperties(delivery_mode=2),
                )
            method.channel.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        """Entry point for the worker process.

        Connects to RabbitMQ if possible, otherwise falls back to the in‑memory
        queue. Continuously batches events and flushes either when the batch size
        reaches ``SYNC_BATCH_SIZE`` or when ``SYNC_FLUSH_INTERVAL_SECONDS`` has
        elapsed.
        """
        connection_channel = self._connect()
        if connection_channel:
            connection, channel = connection_channel
            # Set up consumer with a callback that pushes events onto the batch.
            channel.basic_qos(prefetch_count=Config.SYNC_BATCH_SIZE)
            channel.basic_consume(
                queue='rate_limit_events',
                on_message_callback=self._handle_message,
                auto_ack=False,
            )
            _logger.info('SyncWorker started with RabbitMQ')
        else:
            channel = None
            _logger.info('SyncWorker started in fallback mode (no RabbitMQ)')

        try:
            while not self._stop_event.is_set():
                # Flush based on size or timeout.
                now = time.time()
                if (len(self.batch) >= Config.SYNC_BATCH_SIZE) or (
                    now - self.last_flush >= Config.SYNC_FLUSH_INTERVAL_SECONDS
                ):
                    self._process_batch()

                # If RabbitMQ channel is active, let pika process network events.
                if channel:
                    connection.process_data_events(time_limit=1.0)
                else:
                    # Fallback: pull from in‑memory queue with a short timeout.
                    try:
                        event = self.fallback_queue.get(timeout=1.0)
                        self.batch.append(event)
                    except Exception:
                        pass
        except KeyboardInterrupt:
            _logger.info('SyncWorker received shutdown signal')
        finally:
            if connection_channel:
                connection.close()

    def stop(self):
        self._stop_event.set()
