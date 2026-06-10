"""Operational endpoints: leader election info and system metrics."""

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.evaluate, pytest.mark.asyncio]


async def test_leader_endpoint_returns_holder(client):
    r = await client.get("/api/v1/admin/leader")
    assert r.status_code == 200
    body = r.json()
    assert "leader" in body
    leader = body["leader"]
    assert leader is not None, "No scheduler has acquired leadership"
    assert "holder_id" in leader


async def test_metrics_endpoint_returns_expected_keys(client):
    r = await client.get("/api/v1/metrics")
    assert r.status_code == 200
    body = r.json()
    assert "jobs" in body
    assert "queue" in body
    assert "workers" in body
