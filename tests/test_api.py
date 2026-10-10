import json
import os
import pytest
from app import app
from services.redis_service import RedisService

@pytest.fixture(autouse=True)
def client():
    # Flask test client
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

def test_root_endpoint(client):
    resp = client.get('/')
    assert resp.status_code == 200
    data = resp.get_json()
    assert data.get('message') == 'Edge-Based Distributed Rate Limiting Middleware'

def test_health_endpoints(client):
    # live
    resp = client.get('/health/live')
    assert resp.status_code == 200
    assert resp.get_json().get('status') == 'live'
    # ready (Redis not available -> unready)
    resp = client.get('/health/ready')
    assert resp.status_code == 200
    json_data = resp.get_json()
    assert json_data.get('status') in ('ready', 'unready')

def test_config_endpoint_success(client):
    payload = {
        "algorithm": "token_bucket",
        "limit": 5,
        "bucket_capacity": 5,
        "refill_rate": 1
    }
    resp = client.post('/api/config', json=payload)
    assert resp.status_code == 200
    data = resp.get_json()
    assert 'policy_id' in data

def test_config_endpoint_invalid(client):
    payload = {"algorithm": "unknown"}
    resp = client.post('/api/config', json=payload)
    assert resp.status_code == 400
    assert 'error' in resp.get_json()

def test_request_endpoint_allow_and_block(client):
    # Set a token bucket policy first
    payload = {
        "algorithm": "token_bucket",
        "limit": 5,
        "bucket_capacity": 5,
        "refill_rate": 1
    }
    resp = client.post('/api/config', json=payload)
    assert resp.status_code == 200
    policy_id = resp.get_json()['policy_id']
    # Make 5 allowed requests
    for i in range(5):
        req = {
            "user_id": "user1",
            "edge_id": "edgeA",
            "request_id": f"req{i}"
        }
        r = client.post('/api/request', json=req)
        assert r.status_code == 200
        assert r.get_json()['allowed'] is True
    # 6th should be blocked (429)
    req = {"user_id": "user1", "edge_id": "edgeA", "request_id": "req6"}
    r = client.post('/api/request', json=req)
    assert r.status_code == 429
    assert r.get_json()['allowed'] is False

def test_metrics_endpoint(client):
    resp = client.get('/api/metrics')
    assert resp.status_code == 200
    data = resp.get_json().get('metrics')
    # Metrics may be empty initially, but keys exist
    for key in ['total', 'allowed', 'rejected']:
        assert key in data
