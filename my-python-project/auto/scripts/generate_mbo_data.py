"""
Generate Sample MBO Data for Testing
File: scripts/generate_mbo_data.py

Creates realistic Market-By-Order data files
"""
import csv
import random
from pathlib import Path
from typing import List, Dict


class MBODataGenerator:
    """Generate realistic MBO data"""
    
    def __init__(self, seed: int = 42):
        random.seed(seed)
        self.order_id_counter = 0
        self.active_orders = {}
        self.symbols = ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN"]
        self.base_prices = {
            "AAPL": 180.0,
            "MSFT": 380.0,
            "GOOGL": 140.0,
            "TSLA": 250.0,
            "AMZN": 170.0
        }
    
    def _next_order_id(self) -> str:
        self.order_id_counter += 1
        return f"ORD{self.order_id_counter:010d}"
    
    def _get_price(self, symbol: str, side: str) -> float:
        """Generate realistic price"""
        base = self.base_prices.get(symbol, 100.0)
        
        if side == "bid":
            # Bids slightly below base
            offset = random.uniform(0, 2.0)
            return round(base - offset, 2)
        else:
            # Asks slightly above base
            offset = random.uniform(0, 2.0)
            return round(base + offset, 2)
    
    def _get_quantity(self) -> int:
        """Generate realistic quantity"""
        # Mix of sizes
        if random.random() < 0.7:
            return random.randint(1, 100)  # Retail
        else:
            return random.randint(100, 1000)  # Institutional
    
    def generate_new_order(self, timestamp: int) -> Dict:
        """Generate NEW order"""
        order_id = self._next_order_id()
        symbol = random.choice(self.symbols)
        side = random.choice(["bid", "ask"])
        price = self._get_price(symbol, side)
        size = self._get_quantity()
        
        # Track active orders
        self.active_orders[order_id] = {
            "symbol": symbol,
            "side": side,
            "price": price,
            "size": size
        }
        
        return {
            "timestamp": timestamp,
            "type": "NEW",
            "order_id": order_id,
            "symbol": symbol,
            "side": side,
            "price": price,
            "size": size
        }
    
    def generate_cancel_order(self, timestamp: int) -> Dict:
        """Generate CANCEL order"""
        if not self.active_orders:
            return self.generate_new_order(timestamp)
        
        order_id = random.choice(list(self.active_orders.keys()))
        order = self.active_orders[order_id]
        del self.active_orders[order_id]
        
        return {
            "timestamp": timestamp,
            "type": "CANCEL",
            "order_id": order_id,
            "symbol": order["symbol"],
            "side": order["side"],
            "price": 0,
            "size": 0
        }
    
    def generate_modify_order(self, timestamp: int) -> Dict:
        """Generate MODIFY order"""
        if not self.active_orders:
            return self.generate_new_order(timestamp)
        
        order_id = random.choice(list(self.active_orders.keys()))
        order = self.active_orders[order_id]
        
        # Modify size
        new_size = max(1, order["size"] + random.randint(-50, 50))
        order["size"] = new_size
        
        return {
            "timestamp": timestamp,
            "type": "MODIFY",
            "order_id": order_id,
            "symbol": order["symbol"],
            "side": order["side"],
            "price": order["price"],
            "size": new_size
        }
    
    def generate_execute_order(self, timestamp: int) -> Dict:
        """Generate EXECUTE (fill) order"""
        if not self.active_orders:
            return self.generate_new_order(timestamp)
        
        order_id = random.choice(list(self.active_orders.keys()))
        order = self.active_orders[order_id]
        
        # Execute partial or full
        execute_size = random.randint(1, order["size"])
        order["size"] -= execute_size
        
        # Remove if fully filled
        if order["size"] == 0:
            del self.active_orders[order_id]
        
        return {
            "timestamp": timestamp,
            "type": "EXECUTE",
            "order_id": order_id,
            "symbol": order["symbol"],
            "side": order["side"],
            "price": order["price"],
            "size": execute_size
        }
    
    def generate_messages(self, count: int) -> List[Dict]:
        """Generate sequence of MBO messages"""
        messages = []
        
        # Start with new orders to build book
        for i in range(min(200, count)):
            messages.append(self.generate_new_order(i))
        
        # Mix of operations
        for i in range(200, count):
            op = random.choices(
                ["NEW", "CANCEL", "MODIFY", "EXECUTE"],
                weights=[0.5, 0.2, 0.15, 0.15]
            )[0]
            
            if op == "NEW":
                msg = self.generate_new_order(i)
            elif op == "CANCEL":
                msg = self.generate_cancel_order(i)
            elif op == "MODIFY":
                msg = self.generate_modify_order(i)
            else:
                msg = self.generate_execute_order(i)
            
            messages.append(msg)
        
        return messages
    
    def save_to_csv(self, messages: List[Dict], filepath: str):
        """Save messages to CSV"""
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        
        with open(filepath, 'w', newline='') as f:
            writer = csv.writer(f)
            
            # Header
            writer.writerow([
                'timestamp', 'type', 'order_id', 
                'symbol', 'side', 'price', 'size'
            ])
            
            # Data
            for msg in messages:
                writer.writerow([
                    msg['timestamp'],
                    msg['type'],
                    msg['order_id'],
                    msg['symbol'],
                    msg['side'],
                    msg['price'],
                    msg['size']
                ])
        
        print(f"✓ Generated {len(messages):,} messages -> {filepath}")


def main():
    """Generate sample datasets"""
    generator = MBODataGenerator(seed=42)
    
    datasets = [
        ("data/mbo_small.csv", 1_000),
        ("data/mbo_medium.csv", 10_000),
        ("data/mbo_large.csv", 100_000),
        ("data/mbo_data.csv", 50_000),  # Default
    ]
    
    print("Generating MBO data files...")
    print("="*60)
    
    for filepath, count in datasets:
        messages = generator.generate_messages(count)
        generator.save_to_csv(messages, filepath)
    
    print("="*60)
    print("✓ All datasets generated!")
    print()
    print("Test with:")
    print("  python -m my_package.cli --mode stream --input data/mbo_small.csv")


if __name__ == "__main__":
    main()