from my_package.orderbook import OrderBook
import pytest


def test_no_crossing():
    """Exchange rule: highest bid must be < lowest ask."""
    ob = OrderBook()
    ob.apply({"symbol": "ABC", "side": "bid", "price": 100.0, "size": 10})
    ob.apply({"symbol": "ABC", "side": "ask", "price": 99.0, "size": 5})
    result = ob.verify_correctness("ABC")
    assert not result["valid"]
    assert any("Crossing" in v for v in result["violations"])


def test_valid_book():
    """Valid order book state."""
    ob = OrderBook()
    ob.apply({"symbol": "ABC", "side": "bid", "price": 100.0, "size": 10})
    ob.apply({"symbol": "ABC", "side": "ask", "price": 101.0, "size": 5})
    result = ob.verify_correctness("ABC")
    assert result["valid"]


def test_no_negative_price():
    """Reject negative prices."""
    ob = OrderBook()
    with pytest.raises(ValueError):
        ob.apply({"symbol": "ABC", "side": "bid", "price": -100.0, "size": 10})


def test_audit_trail():
    """Audit log tracks all operations."""
    ob = OrderBook()
    ob.apply({"symbol": "ABC", "side": "bid", "price": 100.0, "size": 10})
    ob.apply({"symbol": "ABC", "side": "bid", "price": 100.0, "size": 0})
    assert len(ob.audit_log) == 2
    assert ob.audit_log[0]["action"] == "add"
    assert ob.audit_log[1]["action"] == "remove"