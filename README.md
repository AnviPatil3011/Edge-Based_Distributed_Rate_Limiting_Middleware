# Edge-Based Distributed Rate Limiting Middleware

## Overview

This project implements edge-based distributed rate limiting using Redis for rate-limit state and RabbitMQ for asynchronous event synchronization.

### Supported algorithms

- Token Bucket
- Sliding Window

### Features

- Atomic Lua scripts for rate-limit operations.
- Flask API for requests, configuration, health checks, metrics and request history.
- RabbitMQ-based asynchronous synchronization worker.
- Redis and RabbitMQ health/status endpoints.
- Dashboard for viewing metrics, infrastructure status and recent requests.
- Docker Compose setup for running the complete application.

## Prerequisites

- Git
- Docker Desktop
- Docker Compose (included with current Docker Desktop versions)

For local Python execution, you also need Python and the dependencies in `requirements.txt`, plus locally running Redis and RabbitMQ services.

## Quick Start — Docker (Recommended)

### 1. Clone the repository

```powershell
git clone https://github.com/AnviPatil3011/Edge-Based_Distributed_Rate_Limiting_Middleware.git
cd Edge-Based_Distributed_Rate_Limiting_Middleware
```

If you have already cloned the repository, open PowerShell in that project folder instead.

### 2. Start Docker Desktop

Wait until Docker Desktop shows that its engine is running.

### 3. Build and start the services

```powershell
docker compose up --build -d
```

This starts Redis, RabbitMQ, the Flask application and the synchronization worker.

### 4. Check the services

```powershell
docker compose ps
docker compose logs --tail 50 app worker
```

### 5. Open the dashboard

Visit:

http://localhost:5000/dashboard

The Flask API is available at:

http://localhost:5000/

### 6. Stop the services

```powershell
docker compose down
```

This stops the containers but normally preserves no persistent application state because this Compose setup does not configure persistent data volumes.

## Useful API Endpoints

- `GET /` — application information
- `GET /health/live` — liveness check
- `GET /health/ready` — readiness check
- `GET /api/config` — current configuration
- `POST /api/config` — update rate-limiting configuration
- `POST /api/request` — submit a rate-limit request
- `POST /api/simulate` — simulate requests
- `GET /api/metrics` — aggregate metrics
- `GET /api/history` — recent request history
- `GET /api/sync/status` — synchronization status

RabbitMQ management UI:

http://localhost:15672/

The development credentials configured in `docker-compose.yml` are:

- Username: `rateuser`
- Password: `ratepass123`

These credentials are for local development only. Do not use them in production.

## Testing

Run the test suite from the project directory:

```powershell
python -m pytest -v
```

Check the actual pytest summary to see how many tests passed. Tests that require external services may need Redis or other dependencies to be available.

## Configuration

`.env.example` documents example environment variables, including Redis and RabbitMQ URLs, default rate limits, synchronization settings and logging.

It is a reference file and is not automatically loaded by Python. Docker Compose supplies its own environment variables to the application and worker.

For local execution, make sure your Redis and RabbitMQ services are running and that the environment variables match your local services.

## License

MIT
