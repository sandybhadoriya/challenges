# tests/test_high_performance.py

import pytest
import time
from my_package.orderbook import OrderBook

class TestHighPerformance:
    """Test 50k+ msg/sec throughput"""
    
    def test_50k_messages_per_second(self):
        """Verify can process 50k messages in 1 second"""
        order_book = OrderBook()
        
        # Generate 50k messages
        messages = []
        for i in range(50000):
            messages.append({
                "symbol": "TEST",
                "side": "bid" if i % 2 == 0 else "ask",
                "price": 100.0 + (i % 100) * 0.01,
                "size": 10
            })
        
        # Process and time
        start = time.time()
        for msg in messages:
            order_book.apply(msg)
        elapsed = time.time() - start
        
        throughput = len(messages) / elapsed
        
        print(f"\n Throughput: {throughput:,.0f} msg/s")
        print(f"p99 latency: {order_book.get_p99_latency():.3f}ms")
        
        # Assertions
        assert throughput > 50000, f"Throughput too low: {throughput:,.0f}"
        assert order_book.get_p99_latency() < 50, "p99 latency too high"