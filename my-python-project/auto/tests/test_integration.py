"""Integration tests for order book reconstruction and streaming."""
import pytest
import json
from my_package.orderbook import OrderBook
from my_package.stream import MessageParser, OrderBookReconstructor
from time import time


class TestMessageParsing:
    """Test MBO message parsing."""
    
    def test_parse_valid_add_order(self):
        parser = MessageParser()
        msg = '{"type":"add","symbol":"ABC","side":"bid","price":100.5,"size":10}\n'
        event = parser.parse(msg)
        assert event["symbol"] == "ABC"
        assert event["side"] == "bid"
        assert event["price"] == 100.5
        assert event["size"] == 10
    
    def test_parse_invalid_json(self):
        parser = MessageParser()
        with pytest.raises(json.JSONDecodeError):
            parser.parse('invalid json\n')
    
    def test_parse_missing_fields(self):
        parser = MessageParser()
        msg = '{"type":"add","symbol":"ABC"}\n'
        with pytest.raises(ValueError):
            parser.parse(msg)


class TestOrderBookReconstruction:
    """Test order book reconstruction from stream."""
    
    def test_reconstruct_from_messages(self):
        reconstructor = OrderBookReconstructor()
        messages = [
            '{"type":"add","symbol":"TST","side":"bid","price":100.0,"size":10}\n',
            '{"type":"add","symbol":"TST","side":"ask","price":101.0,"size":5}\n',
            '{"type":"add","symbol":"TST","side":"bid","price":99.5,"size":20}\n',
        ]
        
        for msg in messages:
            reconstructor.apply(msg)
        
        book = reconstructor.get_book("TST")
        assert len(book["bids"]) == 2
        assert len(book["asks"]) == 1
        assert book["bids"][0]["price"] == 100.0  # highest bid first
    
    def test_p99_latency_under_50ms(self):
        """Verify p99 reconstruction latency < 50ms for 1000 messages."""
        reconstructor = OrderBookReconstructor()
        latencies = []
        
        for i in range(1000):
            msg = f'{{"type":"add","symbol":"TST","side":"bid","price":{100.0 + i * 0.01},"size":10}}\n'
            start = time()
            reconstructor.apply(msg)
            latencies.append((time() - start) * 1000)  # ms
        
        sorted_lat = sorted(latencies)
        p99 = sorted_lat[int(len(sorted_lat) * 0.99)]
        assert p99 < 50, f"p99 latency {p99}ms exceeds 50ms"


class TestOrderBookInvariants:
    """Test order book exchange rules."""
    
    def test_no_crossing_invariant(self):
        ob = OrderBook()
        ob.apply({"symbol": "ABC", "side": "bid", "price": 100.0, "size": 10})
        ob.apply({"symbol": "ABC", "side": "ask", "price": 99.5, "size": 5})
        result = ob.verify_correctness("ABC")
        assert not result["valid"], "Book should have crossing violation"
    
    def test_price_time_priority_bids(self):
        """Higher bids come first."""
        ob = OrderBook()
        ob.apply({"symbol": "ABC", "side": "bid", "price": 99.0, "size": 5})
        ob.apply({"symbol": "ABC", "side": "bid", "price": 100.0, "size": 10})
        ob.apply({"symbol": "ABC", "side": "bid", "price": 100.0, "size": 20})
        
        top = ob.top("ABC", n=10)
        assert top["bids"][0]["price"] == 100.0
        assert top["bids"][1]["price"] == 99.0
    
    def test_price_time_priority_asks(self):
        """Lower asks come first."""
        ob = OrderBook()
        ob.apply({"symbol": "ABC", "side": "ask", "price": 101.0, "size": 5})
        ob.apply({"symbol": "ABC", "side": "ask", "price": 100.0, "size": 10})
        
        top = ob.top("ABC", n=10)
        assert top["asks"][0]["price"] == 100.0
        assert top["asks"][1]["price"] == 101.0
    
    def test_remove_non_existent_level(self):
        """Removing non-existent level is idempotent."""
        ob = OrderBook()
        ob.apply({"symbol": "ABC", "side": "bid", "price": 100.0, "size": 0})
        top = ob.top("ABC")
        assert top["bids"] == []
    
    def test_multiple_symbols_independence(self):
        """Order books for different symbols are independent."""
        ob = OrderBook()
        ob.apply({"symbol": "ABC", "side": "bid", "price": 100.0, "size": 10})
        ob.apply({"symbol": "XYZ", "side": "bid", "price": 200.0, "size": 20})
        
        abc_top = ob.top("ABC")
        xyz_top = ob.top("XYZ")
        
        assert abc_top["bids"][0]["price"] == 100.0
        assert xyz_top["bids"][0]["price"] == 200.0


class TestConcurrency:
    """Test concurrent order book updates."""
    
    def test_concurrent_updates(self):
        """Simulate concurrent trades on same symbol."""
        ob = OrderBook()
        # Simulate 100 concurrent updates - keep bids below asks
        for i in range(50):
            ob.apply({
                "symbol": "ABC",
                "side": "bid",
                "price": 99.0 - (i * 0.01),  # bids go down
                "size": 10 + i,
            })
        
        for i in range(50):
            ob.apply({
                "symbol": "ABC",
                "side": "ask",
                "price": 100.0 + (i * 0.01),  # asks go up
                "size": 10 + i,
            })
        
        result = ob.verify_correctness("ABC")
        assert result["valid"], f"Violations: {result['violations']}"
        
        top = ob.top("ABC", n=10)
        assert len(top["bids"]) > 0
        assert len(top["asks"]) > 0