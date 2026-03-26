"""Health endpoint tests."""

import requests

from conftest import ADAPTER_URL


def test_health_returns_ok():
    r = requests.get(f"{ADAPTER_URL}/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"


def test_unknown_get_returns_404():
    r = requests.get(f"{ADAPTER_URL}/unknown")
    assert r.status_code == 404


def test_unknown_post_returns_404():
    r = requests.post(f"{ADAPTER_URL}/unknown", json={})
    assert r.status_code == 404
