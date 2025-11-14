"""End-to-end API tests."""
from typing import AsyncGenerator
import pytest
from httpx import AsyncClient, ASGITransport
from my_package.server import app


@pytest.fixture
async def client() -> AsyncGenerator[AsyncClient, None]:
    """Create ASGI httpx async client for tests."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as ac:
        yield ac


class TestE2EFlow:
    """Complete user workflow."""

    async def test_full_trading_flow(self, client: AsyncClient):
        """Simulate realistic trading scenario."""
        resp = await client.post("/ingest", json={
            "symbol": "AAPL",
            "side": "bid",
            "price": 150.0,
            "size": 100,
        })
        assert resp.status_code == 200

        resp = await client.post("/ingest", json={
            "symbol": "AAPL",
            "side": "ask",
            "price": 151.0,
            "size": 50,
        })
        assert resp.status_code == 200

        resp = await client.get("/book/AAPL?depth=5")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["bids"]) == 1
        assert len(data["asks"]) == 1

    async def test_metrics_endpoint(self, client: AsyncClient):
        """Verify metrics are collected."""
        for i in range(10):
            await client.post("/ingest", json={
                "symbol": "TEST",
                "side": "bid",
                "price": 100.0 + i,
                "size": 10,
            })

        resp = await client.get("/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, dict)

    async def test_verify_endpoint(self, client: AsyncClient):
        """Check correctness verification."""
        await client.post("/ingest", json={
            "symbol": "CHK",
            "side": "bid",
            "price": 100.0,
            "size": 10,
        })

        resp = await client.get("/verify/CHK")
        assert resp.status_code == 200
        data = resp.json()
        assert data.get("valid") is True

    async def test_crossing_detection(self, client: AsyncClient):
        """Detect crossing orders."""
        await client.post("/ingest", json={
            "symbol": "CROSS",
            "side": "bid",
            "price": 100.0,
            "size": 10,
        })
        await client.post("/ingest", json={
            "symbol": "CROSS",
            "side": "ask",
            "price": 99.0,
            "size": 5,
        })

        resp = await client.get("/verify/CROSS")
        assert resp.status_code == 200
        data = resp.json()
        assert data.get("valid") is False
        assert any("Crossing" in v for v in data.get("violations", []))
