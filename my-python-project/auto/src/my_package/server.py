from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
import uvicorn
from time import time
import logging

from my_package.orderbook import OrderBook
from my_package import db, config, metrics
from my_package.logging_config import logger

app = FastAPI(title="Order Book Demo", version="1.0.0")
_book = OrderBook()


class OrderEvent(BaseModel):
    symbol: str
    side: str
    price: float
    size: int


class ErrorResponse(BaseModel):
    error: str
    request_id: str


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    """Record latency and throughput."""
    start = time()
    response = await call_next(request)
    latency_ms = (time() - start) * 1000
    metrics.record_latency(latency_ms)
    metrics.record_message()
    response.headers["X-Process-Time"] = str(latency_ms)
    logger.info(f"Request {request.url.path} completed in {latency_ms:.2f}ms")
    return response


@app.on_event("startup")
def startup():
    db.init_db(config.db_path)
    logger.info(f"Started in {config.environment} mode")


@app.get("/health")
def health():
    return {
        "status": "ok",
        "environment": config.environment,
        "version": "1.0.0",
    }


@app.get("/metrics")
def get_metrics():
    """Observability: return latency percentiles and throughput."""
    return metrics.get_stats()


@app.post("/ingest")
def ingest(event: OrderEvent, persist: Optional[bool] = Query(True)):
    """
    Ingest an order event with idempotent semantics.
    Returns 200 on success.
    """
    try:
        e = event.dict()
        side = e["side"].lower()
        if side not in ("bid", "ask"):
            metrics.record_error()
            raise HTTPException(status_code=400, detail="side must be 'bid' or 'ask'")
        
        _book.apply(e)
        if persist:
            db.persist_event(e)
        
        return {"result": "applied", "event": e}
    except ValueError as ex:
        metrics.record_error()
        logger.error(f"Validation error: {ex}")
        raise HTTPException(status_code=400, detail=str(ex))
    except Exception as ex:
        metrics.record_error()
        logger.error(f"Unexpected error: {ex}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/book/{symbol}")
def book(symbol: str, depth: Optional[int] = 5):
    """Retrieve top N levels of order book."""
    if depth < 1 or depth > 100:
        raise HTTPException(status_code=400, detail="depth must be 1-100")
    return _book.top(symbol, n=depth)


@app.get("/verify/{symbol}")
def verify(symbol: str):
    """Correctness verification: check exchange rules."""
    return _book.verify_correctness(symbol)


def run(host: str = None, port: int = None):
    host = host or config.host
    port = port or config.port
    uvicorn.run(
        "my_package.server:app",
        host=host,
        port=port,
        log_level=config.log_level,
        workers=config.max_workers,
    )