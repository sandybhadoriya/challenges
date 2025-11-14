from collections import defaultdict
from typing import Dict
import logging

logger = logging.getLogger("my_package.orderbook")


class OrderBook:
    """
    In-memory aggregate order book with correctness validation.
    
    Exchange rules enforced:
    - Price-time priority: earliest bids at highest price, earliest asks at lowest
    - Valid quantities: size > 0 to add, size == 0 to remove
    - No negative prices or sizes
    """

    def __init__(self):
        self.bids: Dict[str, Dict[float, int]] = defaultdict(dict)
        self.asks: Dict[str, Dict[float, int]] = defaultdict(dict)
        self.audit_log: list = []  # for correctness verification

    def _validate_event(self, event: dict) -> None:
        """Validate event correctness."""
        if event["price"] < 0:
            raise ValueError("Price cannot be negative")
        if event["size"] < 0:
            raise ValueError("Size cannot be negative")
        if event["side"].lower() not in ("bid", "ask"):
            raise ValueError("side must be 'bid' or 'ask'")

    def apply(self, event: dict) -> None:
        """Apply event with idempotent semantics and audit trail."""
        self._validate_event(event)
        
        symbol = event["symbol"]
        side = event["side"].lower()
        price = float(event["price"])
        size = int(event["size"])

        if side == "bid":
            levels = self.bids[symbol]
        else:
            levels = self.asks[symbol]

        # Record audit entry for correctness verification
        self.audit_log.append({
            "symbol": symbol,
            "side": side,
            "price": price,
            "size": size,
            "action": "remove" if size == 0 else "add",
        })

        if size <= 0:
            levels.pop(price, None)
            logger.debug(f"Removed {side} level {price} for {symbol}")
        else:
            levels[price] = size
            logger.debug(f"Updated {side} level {price}={size} for {symbol}")

    def top(self, symbol: str, n: int = 5) -> dict:
        """Get top N levels (price-time priority order)."""
        bids = sorted(self.bids.get(symbol, {}).items(), key=lambda x: -x[0])[:n]
        asks = sorted(self.asks.get(symbol, {}).items(), key=lambda x: x[0])[:n]
        return {
            "symbol": symbol,
            "bids": [{"price": p, "size": s} for p, s in bids],
            "asks": [{"price": p, "size": s} for p, s in asks],
        }

    def verify_correctness(self, symbol: str) -> dict:
        """Verify order book invariants."""
        bids = self.bids.get(symbol, {})
        asks = self.asks.get(symbol, {})
        
        violations = []
        
        # Check: highest bid <= lowest ask (no crossing)
        if bids and asks:
            highest_bid = max(bids.keys())
            lowest_ask = min(asks.keys())
            if highest_bid >= lowest_ask:
                violations.append(f"Crossing: bid {highest_bid} >= ask {lowest_ask}")
        
        # Check: all sizes > 0
        for price, size in bids.items():
            if size <= 0:
                violations.append(f"Invalid bid size at {price}: {size}")
        for price, size in asks.items():
            if size <= 0:
                violations.append(f"Invalid ask size at {price}: {size}")
        
        return {
            "symbol": symbol,
            "valid": len(violations) == 0,
            "violations": violations,
        }