"""High-performance data streaming and order book reconstruction."""
import json
from typing import Dict, Optional
from time import time
import logging

logger = logging.getLogger("my_package.stream")


class MessageParser:
    """Parse MBO (Market By Order) messages from TCP stream."""
    
    REQUIRED_FIELDS = ["type", "symbol", "side", "price", "size"]
    
    def parse(self, line: str) -> dict:
        """Parse single message line. Raises on invalid format."""
        line = line.strip()
        if not line:
            raise ValueError("Empty line")
        
        data = json.loads(line)  # raises JSONDecodeError
        
        for field in self.REQUIRED_FIELDS:
            if field not in data:
                raise ValueError(f"Missing required field: {field}")
        
        return {
            "symbol": str(data["symbol"]),
            "side": str(data["side"]).lower(),
            "price": float(data["price"]),
            "size": int(data["size"]),
        }


class OrderBookReconstructor:
    """Reconstruct order book from message stream with latency tracking."""
    
    def __init__(self):
        self.books: Dict[str, dict] = {}  # symbol -> {"bids": {price: size}, "asks": {...}}
        self.parser = MessageParser()
        self.message_count = 0
        self.latencies = []
    
    def apply(self, line: str) -> None:
        """Apply message to order book, track latency."""
        start = time()
        
        try:
            event = self.parser.parse(line)
            symbol = event["symbol"]
            
            if symbol not in self.books:
                self.books[symbol] = {"bids": {}, "asks": {}}
            
            side_key = "bids" if event["side"] == "bid" else "asks"
            price = event["price"]
            size = event["size"]
            
            if size <= 0:
                self.books[symbol][side_key].pop(price, None)
            else:
                self.books[symbol][side_key][price] = size
            
            self.message_count += 1
            latency_ms = (time() - start) * 1000
            self.latencies.append(latency_ms)
            
        except (json.JSONDecodeError, ValueError, KeyError) as ex:
            logger.error(f"Parse error: {ex}")
            raise
    
    def get_book(self, symbol: str, n: int = 5) -> dict:
        """Get top N levels for symbol."""
        if symbol not in self.books:
            return {"symbol": symbol, "bids": [], "asks": []}
        
        book = self.books[symbol]
        bids = sorted(book["bids"].items(), key=lambda x: -x[0])[:n]
        asks = sorted(book["asks"].items(), key=lambda x: x[0])[:n]
        
        return {
            "symbol": symbol,
            "bids": [{"price": p, "size": s} for p, s in bids],
            "asks": [{"price": p, "size": s} for p, s in asks],
        }
    
    def get_stats(self) -> dict:
        """Get throughput and latency stats."""
        if not self.latencies:
            return {"messages": 0}
        
        sorted_lat = sorted(self.latencies)
        n = len(sorted_lat)
        
        return {
            "messages_processed": self.message_count,
            "throughput_msg_per_sec": self.message_count / (time()),
            "latencies_ms": {
                "min": min(sorted_lat),
                "max": max(sorted_lat),
                "p50": sorted_lat[n // 2],
                "p99": sorted_lat[int(n * 0.99)],
                "mean": sum(sorted_lat) / n,
            },
        }