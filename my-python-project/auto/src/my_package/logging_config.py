import logging
import json
from datetime import datetime, timezone
import pytest
import asyncio
import httpx
import subprocess
import time
import websockets
from hypothesis import given, strategies as st


class StructuredFormatter(logging.Formatter):
    """Structured JSON logging."""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
        }
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_data)


def setup_logging(level: str = "info"):
    logger = logging.getLogger("my_package")
    logger.setLevel(level.upper())
    handler = logging.StreamHandler()
    handler.setFormatter(StructuredFormatter())
    logger.addHandler(handler)
    return logger


logger = setup_logging()


async def chunked_generator():
    for i in range(10):
        yield (f'{{"symbol":"S","side":"bid","price":{100+i},"size":1}}\n').encode()
        await asyncio.sleep(0.01)


@pytest.mark.anyio
async def test_chunked_ingest_to_ingest_endpoint():
    async with httpx.AsyncClient(base_url="http://localhost:8000") as client:
        # send chunked POST
        resp = await client.post("/ingest-stream", content=chunked_generator())
        assert resp.status_code == 200
        data = resp.json()
        assert data["ingested"] >= 10


@pytest.fixture(scope="module")
def running_server():
    p = subprocess.Popen(["uvicorn", "my_package.server:app", "--port", "8000"])
    try:
        time.sleep(0.5)  # wait for startup
        yield
    finally:
        p.terminate()
        p.wait()


@pytest.mark.asyncio
async def test_websocket_feed(running_server):
    uri = "ws://localhost:8000/ws/trades"
    async with websockets.connect(uri) as ws:
        # send subscribe request if protocol requires
        await ws.send('{"action":"subscribe","symbol":"AAPL"}')
        msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
        assert "AAPL" in msg
        # consume a few messages
        for _ in range(5):
            m = await asyncio.wait_for(ws.recv(), timeout=1.0)
            assert "price" in m


@pytest.mark.anyio
async def test_sse_metrics_endpoint(running_server):
    async with httpx.AsyncClient(base_url="http://localhost:8000", timeout=None) as client:
        async with client.stream("GET", "/metrics/stream") as rsp:
            assert rsp.status_code == 200
            async for chunk in rsp.aiter_text():
                if chunk.strip():
                    # parse SSE lines (e.g., "data: {...}")
                    if "data:" in chunk:
                        # basic validation
                        assert "messages_total" in chunk
                        break


from my_package.server import OrderBook

@given(st.lists(st.fixed_dictionaries({
    "symbol": st.just("T"),
    "side": st.sampled_from(["bid","ask"]),
    "price": st.floats(min_value=0.01, max_value=1000),
    "size": st.integers(min_value=0, max_value=1000)
}), min_size=1, max_size=200))
def test_orderbook_invariants(messages):
    ob = OrderBook()
    for m in messages:
        ob.apply(m)
    assert ob.valid("T")