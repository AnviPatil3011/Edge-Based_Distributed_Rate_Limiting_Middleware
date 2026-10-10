# Edge-Based Distributed Rate Limiting Middleware

## Overview

This project implements a high‑performance, edge‑based distributed rate‑limiting middleware using **Redis** for counting and **RabbitMQ** for asynchronous synchronization. It supports two algorithms:
- Token Bucket
- Sliding Window

Key features:
- Atomic Lua scripts for concurrency safety.
- Bounded retry and dead‑letter handling for RabbitMQ messages.
- In‑memory fallback when RabbitMQ is unavailable.
- Flask API exposing health checks, configuration, metrics, request history and synchronization status.
- Dashboard UI (static files) that visualises live data.

## Quick Start (local)

```bash
# Install dependencies
pip install -r requirements.txt

# Run the Flask app
python -m flask run --host=0.0.0.0 --port=5000

# In another terminal, start the sync worker
python -m workers.sync_worker
```

## Docker

A complete Docker Compose setup is provided in `docker-compose.yml`. It spins up Redis, RabbitMQ, the Flask web service and the sync worker.

```bash
docker compose up --build
```

Visit `http://localhost:5000/dashboard` to see the UI.

## Testing

```bash
pytest -vv
```

All unit tests pass (12). Integration tests that require a real Redis/RabbitMQ are marked accordingly.

## Configuration

Environment variables are defined in `.env.example`:
- `REDIS_URL`
- `RABBITMQ_URL`
- `MAX_RETRIES`
- `DEAD_LETTER_QUEUE`
- etc.

## License

MIT
