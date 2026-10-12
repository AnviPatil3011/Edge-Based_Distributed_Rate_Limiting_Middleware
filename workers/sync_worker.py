import json
import logging
import time
import threading
from typing import List

from config import Config
from services.redis_service import RedisService

_logger = logging.getLogger(__name__)

try:
    import pika
except ImportError:
    pika = None
    _logger.debug("pika library not available")


class SyncWorker:
    """Worker that processes rate-limit events in batches."""

    def __init__(self, fallback_queue=None):
        self.fallback_queue = fallback_queue
        self.batch: List[dict] = []
        self.last_flush = time.time()
        self.redis = RedisService()
        self._stop_event = threading.Event()

    def _connect(self):
        if not pika:
            return None

        try:
            params = pika.URLParameters(Config.RABBITMQ_URL)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()

            channel.queue_declare(
                queue="rate_limit_events",
                durable=True,
            )
            channel.queue_declare(
                queue=Config.DEAD_LETTER_QUEUE,
                durable=True,
            )

            return connection, channel

        except Exception as exc:
            _logger.exception("Failed to connect to RabbitMQ: %s", exc)
            return None

    def _process_batch(self):
        if not self.batch:
            return

        try:
            payload = json.dumps(self.batch)

            self.redis.execute_lua(
                "SYNC_BATCH",
                keys=[
                    "sync:processed_ids",
                    "sync:processed_events",
                    "sync:processed_batches",
                    "sync",
                ],
                args=[payload],
            )

            self.redis.client.set(
                "worker:heartbeat",
                int(time.time()),
            )

            _logger.info(
                "Processed sync batch of %d events",
                len(self.batch),
            )

            self.batch.clear()
            self.last_flush = time.time()

        except Exception:
            _logger.exception(
                "Failed to process sync batch; retaining batch for retry"
            )

    def _handle_message(self, channel, method, properties, body):
        try:
            event = json.loads(body)
        except Exception:
            _logger.warning("Invalid JSON message; discarding")
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        headers = getattr(properties, "headers", None) or {}
        retry_count = headers.get("x-retry-count", 0)

        try:
            self.batch.append(event)
            channel.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as exc:
            _logger.exception("Error handling event: %s", exc)

            if retry_count < Config.MAX_RETRIES:
                new_properties = pika.BasicProperties(
                    delivery_mode=2,
                    headers={"x-retry-count": retry_count + 1},
                )

                channel.basic_publish(
                    exchange="",
                    routing_key="rate_limit_events",
                    body=body,
                    properties=new_properties,
                )
            else:
                channel.basic_publish(
                    exchange="",
                    routing_key=Config.DEAD_LETTER_QUEUE,
                    body=body,
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                    ),
                )

            channel.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        connection_channel = self._connect()

        if connection_channel:
            connection, channel = connection_channel

            channel.basic_qos(
                prefetch_count=Config.SYNC_BATCH_SIZE,
            )

            channel.basic_consume(
                queue="rate_limit_events",
                on_message_callback=self._handle_message,
                auto_ack=False,
            )

            _logger.info("SyncWorker started with RabbitMQ")

        else:
            connection = None
            channel = None
            _logger.info("SyncWorker started without RabbitMQ")


        try:
            while not self._stop_event.is_set():
                now = time.time()

                # Update heartbeat even when no events are available.
                try:
                    self.redis.client.set(
                        "worker:heartbeat",
                        int(now),
                    )
                except Exception:
                    _logger.exception("Failed to update worker heartbeat")

                if (
                    len(self.batch) >= Config.SYNC_BATCH_SIZE
                    or now - self.last_flush
                    >= Config.SYNC_FLUSH_INTERVAL_SECONDS
                ):
                    self._process_batch()

                if channel:
                    connection.process_data_events(time_limit=1.0)

                elif self.fallback_queue is not None:
                    try:
                        event = self.fallback_queue.get(timeout=1.0)
                        self.batch.append(event)
                    except Exception:
                        pass

                else:
                    time.sleep(1.0)

        except KeyboardInterrupt:
            _logger.info("Worker stopping")

        finally:
            if (
                connection_channel
                and connection
                and connection.is_open
            ):
                connection.close()

    def stop(self):
        self._stop_event.set()