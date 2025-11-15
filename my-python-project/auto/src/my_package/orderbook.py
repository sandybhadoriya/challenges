# File: src/my_package/orderbook.py
import logging
from typing import Dict, List, Tuple, Optional, Any

# Import config and setup for consistent, structured logging output
try:
    from .config import config
except ImportError:
    class MockConfig:
        log_level = 'INFO'
    config = MockConfig()
    
from .logging import setup_logging
orderbook_logger = setup_logging(config.log_level)


class SingleSymbolBook:
    """
    Manages the Market-By-Order state for a single trading symbol.
    Enforces Price-Time Priority.
    """

    def __init__(self, symbol: str):
        self.symbol = symbol
        
        # Central storage for all active orders: order_id -> message (Dict)
        self._orders: Dict[str, Dict] = {} 
        
        # Price levels: price (float) -> list of orders (Dict), list enforces FIFO (time priority)
        self._bids: Dict[float, List[Dict]] = {} # Bids sorted high-to-low
        self._asks: Dict[float, List[Dict]] = {} # Asks sorted low-to-high

    # --- Core MBO Handlers ---

    def handle_new(self, msg: Dict):
        """Processes a NEW order message."""
        order_id = msg['order_id']
        price = msg['price']
        side = msg['side']
        size = msg['size']

        if order_id in self._orders:
            orderbook_logger.warning(
                f"NEW order ID {order_id} already exists. Ignoring.",
                extra={"symbol": self.symbol}
            )
            return

        # Store the order details
        self._orders[order_id] = msg

        # Add to price level
        level = self._bids if side == 'bid' else self._asks
        if price not in level:
            level[price] = []
        
        # Add the new order to the end of the list to enforce time priority (FIFO)
        level[price].append(msg)
        
        orderbook_logger.debug(
            f"NEW {side} {self.symbol} @ {price}, Size: {size}, ID: {order_id}",
            extra={"symbol": self.symbol}
        )

    def handle_cancel(self, msg: Dict):
        """Processes a CANCEL order message."""
        order_id = msg['order_id']

        if order_id not in self._orders:
            orderbook_logger.warning(
                f"CANCEL received for unknown order ID {order_id}. Ignoring.",
                extra={"symbol": self.symbol}
            )
            return

        order_to_cancel = self._orders.pop(order_id)
        price = order_to_cancel['price']
        side = order_to_cancel['side']

        # Find and remove from price level list (linear search, most common bottleneck)
        level = self._bids if side == 'bid' else self._asks
        
        if price in level:
            try:
                # Iterate over the list to find the exact order and remove it
                level[price].remove(order_to_cancel)
                
                # Cleanup empty price level
                if not level[price]:
                    del level[price]
                    
                orderbook_logger.debug(
                    f"CANCEL {side} {self.symbol} @ {price}, ID: {order_id}",
                    extra={"symbol": self.symbol}
                )
            except ValueError:
                # Should not happen if _orders was correctly updated, but safety check
                orderbook_logger.error(
                    f"Order {order_id} found in _orders but not at price level {price}.",
                    extra={"symbol": self.symbol}
                )
        else:
            orderbook_logger.error(
                f"Order {order_id} found in _orders but price level {price} missing.",
                extra={"symbol": self.symbol}
            )
            
    def handle_modify(self, msg: Dict):
        """
        Processes a MODIFY order message. 
        MBO modifications typically require a CANCEL (old order) followed by a NEW (new order).
        For simplicity and strict time priority adherence, we implement it as:
        1. Cancel the old order.
        2. Insert the modified order as a NEW order (loses time priority).
        """
        order_id = msg['order_id']
        new_size = msg['size']
        
        if order_id not in self._orders:
            orderbook_logger.warning(
                f"MODIFY received for unknown order ID {order_id}. Treating as NEW/Invalid.",
                extra={"symbol": self.symbol}
            )
            self.handle_new(msg) # Treat as new if not found
            return
            
        old_order = self._orders.get(order_id, {})
        old_price = old_order.get('price')
        old_side = old_order.get('side')
        
        if old_price is None or old_side is None:
            orderbook_logger.error(f"Malformed old order data for ID {order_id}. Cannot modify.")
            return

        # 1. Remove old order (like a CANCEL)
        cancel_msg = {'order_id': order_id, 'price': old_price, 'side': old_side}
        # Note: We manually call the logic instead of `handle_cancel` to manage the pop/logging.
        
        self._orders.pop(order_id)
        level = self._bids if old_side == 'bid' else self._asks
        
        if old_price in level:
            # We must find the specific object to remove to avoid removing a duplicate
            try:
                level[old_price].remove(old_order)
                if not level[old_price]:
                    del level[old_price]
            except ValueError:
                orderbook_logger.error(f"Failed to find order {order_id} in price level during modify.")
        
        # 2. Insert new order with the same ID but updated size/timestamp/price
        # Update the message with the old order's price/side if not provided in the MODIFY msg
        
        # If the MODIFY message specifies a NEW price/side, use that. If not, retain old.
        # Assuming the incoming 'msg' has the intended new state (price, size, side)
        
        # Overwrite/update the price and size in the message (if the MODIFY is price-preserving)
        # Note: If MODIFY changes price, this is often handled as a CANCEL/NEW with a new order_id.
        # Here, we assume a simple in-place size change:
        msg['size'] = new_size # Size must be the only required change for a true MBO MODIFY
        
        # To strictly enforce time priority loss, we re-add it as NEW.
        # If price/side changed, this is a real NEW order now.
        self.handle_new(msg)
        
        orderbook_logger.debug(
            f"MODIFY {old_side} {self.symbol} ID {order_id}. New Size: {new_size}.",
            extra={"symbol": self.symbol}
        )


    def handle_execute(self, msg: Dict):
        """
        Processes an EXECUTE message, simulating the trade and partial fill.
        This assumes the EXECUTE message identifies the passive order ID being hit.
        """
        order_id = msg['order_id']
        exec_size = msg['size']

        if order_id not in self._orders:
            orderbook_logger.warning(
                f"EXECUTE received for unknown order ID {order_id}. Ignoring.",
                extra={"symbol": self.symbol}
            )
            return

        passive_order = self._orders[order_id]
        passive_size = passive_order['size']
        side = passive_order['side']
        price = passive_order['price']

        if exec_size > passive_size:
            orderbook_logger.warning(
                f"EXECUTE size {exec_size} exceeds order size {passive_size}. Capping.",
                extra={"symbol": self.symbol}
            )
            exec_size = passive_size
            
        # Update size
        new_size = passive_size - exec_size
        passive_order['size'] = new_size
        
        # Check for full fill
        if new_size <= 0:
            orderbook_logger.debug(
                f"EXECUTE fully filled ID {order_id}. Removing order.",
                extra={"symbol": self.symbol}
            )
            # Remove from central storage and price level
            self._orders.pop(order_id)
            level = self._bids if side == 'bid' else self._asks
            
            if price in level:
                try:
                    level[price].remove(passive_order)
                    if not level[price]:
                        del level[price]
                except ValueError:
                    orderbook_logger.error(f"Order {order_id} not found at level {price} during full fill.")
        else:
            orderbook_logger.debug(
                f"EXECUTE partial fill ID {order_id}. Remaining Size: {new_size}.",
                extra={"symbol": self.symbol}
            )


    # --- Utility Methods ---

    def get_bba(self) -> Tuple[float, float, int, int]:
        """
        Calculates the Best Bid and Ask (BBA) for this symbol.
        Returns: (best_bid, best_ask, bid_size, ask_size)
        """
        best_bid = max(self._bids.keys()) if self._bids else 0.0
        best_ask = min(self._asks.keys()) if self._asks else 0.0
        
        bid_size = sum(order['size'] for price in self._bids for order in self._bids[price])
        ask_size = sum(order['size'] for price in self._asks for order in self._asks[price])

        # Optimize: Only calculate size for the best price levels
        if best_bid > 0:
            bid_size = sum(order['size'] for order in self._bids[best_bid])
        else:
            bid_size = 0
            
        if best_ask > 0:
            ask_size = sum(order['size'] for order in self._asks[best_ask])
        else:
            ask_size = 0
            
        return best_bid, best_ask, bid_size, ask_size

    def get_top_of_book(self) -> Dict[str, Any]:
        """Returns the BBA and the depth (size at best price) for logging/metrics."""
        best_bid, best_ask, bid_size, ask_size = self.get_bba()
        return {
            'symbol': self.symbol,
            'best_bid': best_bid,
            'best_ask': best_ask,
            'bid_depth': bid_size,
            'ask_depth': ask_size,
            'spread': best_ask - best_bid if best_ask > 0 and best_bid > 0 else 0.0,
        }

    def get_book_depth(self) -> Dict[str, Any]:
        """Returns the full depth of the order book for both sides."""
        return {
            'symbol': self.symbol,
            'bids': {price: [order['size'] for order in orders] for price, orders in self._bids.items()},
            'asks': {price: [order['size'] for order in orders] for price, orders in self._asks.items()},
            'total_orders': len(self._orders)
        }


class OrderBook:
    """
    The main OrderBook system managing multiple SingleSymbolBooks.
    This is the entry point for MBO message processing.
    """
    
    def __init__(self):
        # Maps symbol (str) to SingleSymbolBook instance
        self._books: Dict[str, SingleSymbolBook] = {}

    def apply(self, message: Dict):
        """
        The central method to process an MBO message against the correct Order Book.
        """
        symbol = message.get('symbol')
        msg_type = message.get('type')
        
        if not symbol or not msg_type:
            orderbook_logger.error(f"Message missing required fields (symbol/type): {message}")
            return
            
        # Ensure the book exists for this symbol
        if symbol not in self._books:
            self._books[symbol] = SingleSymbolBook(symbol)
            orderbook_logger.info(f"Created new order book for symbol: {symbol}")
            
        book = self._books[symbol]
        
        try:
            if msg_type == 'NEW':
                book.handle_new(message)
            elif msg_type == 'CANCEL':
                book.handle_cancel(message)
            elif msg_type == 'MODIFY':
                book.handle_modify(message)
            elif msg_type == 'EXECUTE':
                book.handle_execute(message)
            else:
                orderbook_logger.warning(f"Unknown message type: {msg_type}")
                
        except Exception as e:
            orderbook_logger.error(
                f"Error processing message {msg_type} for {symbol}: {e}",
                extra={"message": message}
            )

    def get_bba_for_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Retrieves the BBA for a specific symbol."""
        book = self._books.get(symbol)
        if book:
            return book.get_top_of_book()
        return None

    def get_all_symbols(self) -> List[str]:
        """Returns a list of all symbols currently managed."""
        return list(self._books.keys())

    def get_full_depth(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Retrieves the full depth map for a specific symbol."""
        book = self._books.get(symbol)
        if book:
            return book.get_book_depth()
        return None