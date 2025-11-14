from my_package.orderbook import OrderBook

def test_orderbook_apply_and_top():
    ob = OrderBook()
    ob.apply({"symbol": "TST", "side": "bid", "price": 100.0, "size": 10})
    ob.apply({"symbol": "TST", "side": "ask", "price": 101.0, "size": 5})
    top = ob.top("TST", n=2)
    assert top["bids"][0]["price"] == 100.0
    assert top["bids"][0]["size"] == 10
    assert top["asks"][0]["price"] == 101.0
    assert top["asks"][0]["size"] == 5
    ob.apply({"symbol": "TST", "side": "bid", "price": 100.0, "size": 0})
    top2 = ob.top("TST")
    assert top2["bids"] == []