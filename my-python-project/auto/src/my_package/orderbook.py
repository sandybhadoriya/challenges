from collections import defaultdict
from typing import Dict, Any 
import logging
import time 
import statistics 

logger = logging.getLogger("my_package.orderbook")


class OrderBook:
    """
    In-memory aggregate order book with correctness validation and performance tracking.
    """

    def __init__(self):
        self.bids: Dict[str, Dict[float, int]] = defaultdict(dict)
        self.asks: Dict[str, Dict[float, int]] = defaultdict(dict)
        self.audit_log: list = []
        self.latencies: list = [] # Stores processing time in milliseconds

    def _validate_event(self, event: dict) -> None:
        """Validate event correctness."""
        if event["price"] < 0:
            raise ValueError("Price cannot be negative")
        if event["size"] < 0:
            raise ValueError("Size cannot be negative")
        if event["side"].lower() not in ("bid", "ask"):
            raise ValueError("side must be 'bid' or 'ask'")

    def apply(self, event: dict) -> None:
        """Apply event with idempotent semantics, audit trail, and latency tracking."""
        start_time = time.perf_counter_ns() # Start timer

        self._validate_event(event)
        
        symbol = event["symbol"]
        side = event["side"].lower()
        price = float(event["price"])
        size = int(event["size"])

        if side == "bid":
            levels = self.bids[symbol]
        else:
            levels = self.asks[symbol]

        if size <= 0:
            levels.pop(price, None)
        else:
            levels[price] = size

        end_time = time.perf_counter_ns() # End timer
        elapsed_ns = end_time - start_time
        # Store latency in milliseconds
        self.latencies.append(elapsed_ns / 1_000_000)

    def top(self, symbol: str, n: int = 5) -> dict:
        """Get top N levels (price-time priority order)."""
        bids = sorted(self.bids.get(symbol, {}).items(), key=lambda x: -x[0])[:n]
        asks = sorted(self.asks.get(symbol, {}).items(), key=lambda x: x[0])[:n]
        return {
            "symbol": symbol,
            "bids": [{"price": p, "size": s} for p, s in bids],
            "asks": [{"price": p, "size": s} for p, s in asks],
        }

    def get_bids(self, symbol: str) -> Dict[float, int]:
        return dict(self.bids.get(symbol, {}))
    
    def get_asks(self, symbol: str) -> Dict[float, int]:
        return dict(self.asks.get(symbol, {}))
        
    def get_p99_latency(self) -> float:
        """Calculate and return the 99th percentile latency in milliseconds."""
        if not self.latencies:
            return 0.0
        if len(self.latencies) < 100:
            return max(self.latencies)
        return statistics.quantiles(self.latencies, n=100)[98]

    def get_full_book_state(self) -> Dict[str, Any]:
        """
        [MANDATORY DELIVERABLE] Returns the complete state of the order book.
        """
        state = {}
        all_symbols = set(self.bids.keys()) | set(self.asks.keys())
        
        for symbol in all_symbols:
            state[symbol] = {
                # Ensure prices are converted back to strings for JSON keys if needed, 
                # but dict keys in Python are typically floats here.
                "bids": self.get_bids(symbol),
                "asks": self.get_asks(symbol)
            }
        
        return state