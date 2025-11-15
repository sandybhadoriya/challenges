# File: app.py
import asyncio
import uvicorn
import time
from fastapi import FastAPI, HTTPException
from my_package.orderbook import OrderBook
from my_package.tcp_server import MBOFileReader 

# --- Configuration ---
API_PORT = 8000
MBO_FILE_PATH = "data/mbo_data.csv"

# --- Global State ---
ORDER_BOOK = OrderBook()
app = FastAPI(title="Batonics Order Book API")

async def reconstruct_book_on_startup():
    """Reads MBO data and applies it to the OrderBook."""
    try:
        reader = MBOFileReader(MBO_FILE_PATH)
        messages = reader.load()
        print(f"Processing {len(messages)} messages for API state...")
        
        for msg in messages:
            ORDER_BOOK.apply(msg)
            await asyncio.sleep(0) # Yield control
            
        print(f"✅ API Order Book ready.")
        
    except FileNotFoundError:
        print(f"❌ ERROR: MBO file not found at {MBO_FILE_PATH}. API will serve empty data.")
    except Exception as e:
        print(f"❌ ERROR during data processing: {e}")

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(reconstruct_book_on_startup())

@app.get("/api/v1/book/{symbol}", tags=["Order Book"])
async def get_top_of_book(symbol: str, depth: int = 10):
    """Retrieves the top N bids and asks for a specified symbol."""
    if not ORDER_BOOK.get_bids(symbol) and not ORDER_BOOK.get_asks(symbol):
        raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found.")
    
    return ORDER_BOOK.top(symbol, n=depth)

@app.get("/api/v1/metrics", tags=["Metrics"])
async def get_system_metrics():
    """Retrieves key performance (p99 latency) and throughput metrics."""
    return {
        "p99_latency_ms": ORDER_BOOK.get_p99_latency(),
        "messages_processed": len(ORDER_BOOK.latencies),
        "book_symbols": len(ORDER_BOOK.bids.keys() | ORDER_BOOK.asks.keys()),
    }

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=API_PORT, reload=True)