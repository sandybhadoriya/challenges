"""Resilience and error handling tests."""
import pytest
from my_package.orderbook import OrderBook


class TestResilience:
    """Test graceful failure handling."""
    
    def test_invalid_message_continues_processing(self):
        """Invalid message doesn't crash server."""
        ob = OrderBook()
        
        # Valid
        ob.apply({"symbol": "ABC", "side": "bid", "price": 100.0, "size": 10})
        
        # Invalid - caught
        with pytest.raises(ValueError):
            ob.apply({"symbol": "ABC", "side": "invalid", "price": 100.0, "size": 10})
        
        # Valid again - server still works
        ob.apply({"symbol": "ABC", "side": "ask", "price": 101.0, "size": 5})
        
        top = ob.top("ABC")
        assert len(top["bids"]) == 1
        assert len(top["asks"]) == 1
    
    def test_duplicate_event_idempotent(self):
        """Same event twice produces same result (idempotent)."""
        ob = OrderBook()
        
        event = {"symbol": "ABC", "side": "bid", "price": 100.0, "size": 10}
        ob.apply(event)
        top1 = ob.top("ABC")
        
        # Apply again
        ob.apply(event)
        top2 = ob.top("ABC")
        
        assert top1 == top2
    
    def test_removal_idempotent(self):
        """Removing non-existent level is safe."""
        ob = OrderBook()
        
        ob.apply({"symbol": "ABC", "side": "bid", "price": 100.0, "size": 10})
        ob.apply({"symbol": "ABC", "side": "bid", "price": 100.0, "size": 0})
        
        # Remove again (should be no-op)
        ob.apply({"symbol": "ABC", "side": "bid", "price": 100.0, "size": 0})
        
        top = ob.top("ABC")
        assert top["bids"] == []
    
    def test_large_order_quantity(self):
        """Handle very large order sizes."""
        ob = OrderBook()
        
        ob.apply({
            "symbol": "ABC",
            "side": "bid",
            "price": 100.0,
            "size": 1_000_000_000,  # 1 billion
        })
        
        top = ob.top("ABC")
        assert top["bids"][0]["size"] == 1_000_000_000
        result = ob.verify_correctness("ABC")
        assert result["valid"]
    
    def test_audit_trail_completeness(self):
        """All operations recorded in audit log."""
        ob = OrderBook()
        
        events = [
            {"symbol": "ABC", "side": "bid", "price": 100.0, "size": 10},
            {"symbol": "ABC", "side": "bid", "price": 99.0, "size": 20},
            {"symbol": "ABC", "side": "bid", "price": 100.0, "size": 0},
        ]
        
        for e in events:
            ob.apply(e)
        
        assert len(ob.audit_log) == 3
        assert ob.audit_log[2]["action"] == "remove"