"""
TCP Streaming Server for MBO Data
File: src/my_package/tcp_server.py

Integrates with your existing order book
Supports 50k-500k messages/second
"""
import asyncio
import json
import time
import logging
from typing import List, Dict, Optional, Callable
from dataclasses import dataclass
import struct
# Assuming 'from my_package.orderbook import OrderBook' is used in the __main__ block

logger = logging.getLogger(__name__)


@dataclass
class StreamMetrics:
    """Track streaming performance"""
    messages_sent: int = 0
    bytes_sent: int = 0
    start_time: float = 0
    clients_connected: int = 0
    errors: int = 0
    
    def get_throughput(self) -> float:
        elapsed = time.time() - self.start_time
        return self.messages_sent / elapsed if elapsed > 0 else 0
    
    def to_dict(self) -> dict:
        return {
            "messages_sent": self.messages_sent,
            "bytes_sent": self.bytes_sent,
            "throughput_msg_per_sec": round(self.get_throughput(), 2),
            "clients_connected": self.clients_connected,
            "errors": self.errors,
            "uptime_seconds": round(time.time() - self.start_time, 2)
        }


class MBOFileReader:
    """
    Read MBO data from CSV file
    File: src/my_package/mbo_reader.py
    """
    
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.messages = []
    
    def load(self) -> List[Dict]:
        """Load MBO file and parse messages"""
        logger.info(f"Loading MBO file: {self.filepath}")
        
        try:
            with open(self.filepath, 'r') as f:
                # Skip header if present
                header = f.readline().strip()
                
                for line_num, line in enumerate(f, 2):
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    
                    try:
                        msg = self._parse_line(line)
                        if msg:
                            self.messages.append(msg)
                    except Exception as e:
                        logger.warning(f"Line {line_num}: Failed to parse: {e}")
            
            logger.info(f"Loaded {len(self.messages)} messages")
            return self.messages
            
        except FileNotFoundError:
            logger.error(f"File not found: {self.filepath}")
            raise
    
    def _parse_line(self, line: str) -> Optional[Dict]:
        """
        Parse MBO message line
        Expected format: timestamp,type,order_id,symbol,side,price,size
        """
        parts = line.split(',')
        if len(parts) < 7:
            return None
        
        return {
            "timestamp": int(parts[0]),
            "type": parts[1].strip().upper(),
            "order_id": parts[2].strip(),
            "symbol": parts[3].strip(),
            "side": parts[4].strip().lower(),  # Convert to 'bid' or 'ask'
            "price": float(parts[5]),
            "size": int(parts[6])
        }


class TCPStreamServer:
    """
    High-performance TCP streaming server
    Integrates with your existing order book
    """
    
    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 9999,
        target_rate: int = 100000,
        buffer_size: int = 10000
    ):
        self.host = host
        self.port = port
        self.target_rate = target_rate
        self.buffer_size = buffer_size
        self.metrics = StreamMetrics()
        self.clients = []
        self.running = False
    
    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle individual client connection"""
        addr = writer.get_extra_info('peername')
        logger.info(f"Client connected: {addr}")
        
        self.metrics.clients_connected += 1
        self.clients.append(writer)
        
        try:
            # Send welcome message
            await self._send_message(writer, {
                "type": "WELCOME",
                "message": "MBO Stream Server",
                "timestamp": int(time.time() * 1000)
            })
            
            # Keep connection alive
            while self.running:
                if reader.at_eof():
                    break
                await asyncio.sleep(0.1)
                
        except (ConnectionResetError, BrokenPipeError):
            logger.info(f"Client disconnected: {addr}")
        except Exception as e:
            logger.error(f"Error handling client {addr}: {e}")
            self.metrics.errors += 1
        finally:
            if writer in self.clients:
                self.clients.remove(writer)
            self.metrics.clients_connected -= 1
            writer.close()
            try:
                await writer.wait_closed()
            except:
                pass
    
    async def _send_message(self, writer: asyncio.StreamWriter, message: Dict):
        """Send message to client with length prefix"""
        try:
            # Serialize
            json_data = json.dumps(message).encode('utf-8')
            
            # Length-prefixed: 4 bytes length + data
            length = struct.pack('!I', len(json_data))
            
            writer.write(length + json_data)
            await writer.drain()
            
            self.metrics.messages_sent += 1
            self.metrics.bytes_sent += len(json_data) + 4
            
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            self.metrics.errors += 1
            raise
    
    async def broadcast_message(self, message: Dict):
        """Broadcast message to all connected clients"""
        disconnected = []
        
        for writer in self.clients:
            try:
                await self._send_message(writer, message)
            except Exception:
                disconnected.append(writer)
        
        # Clean up disconnected clients
        for writer in disconnected:
            if writer in self.clients:
                self.clients.remove(writer)
                self.metrics.clients_connected -= 1
    
    async def stream_messages(
        self, 
        messages: List[Dict], 
        callback: Optional[Callable] = None
    ):
        """
        Stream messages at target rate with optional callback
        """
        logger.info(f"Starting stream: {len(messages)} messages @ {self.target_rate} msg/s target")
        self.metrics.start_time = time.time()
        
        # Calculate delay to achieve target rate
        delay_per_message = 1.0 / self.target_rate if self.target_rate > 0 else 0
        
        # Process in batches for efficiency
        batch_size = 100
        
        for i in range(0, len(messages), batch_size):
            batch = messages[i:i + batch_size]
            batch_start = time.time()
            
            for message in batch:
                # Apply callback (e.g., update your order book)
                if callback:
                    try:
                        callback(message)
                    except Exception as e:
                        logger.error(f"Callback error: {e}")
                        self.metrics.errors += 1
                
                # Broadcast to clients
                if self.clients:
                    await self.broadcast_message(message)
            
            # Rate limiting
            batch_elapsed = time.time() - batch_start
            expected_time = delay_per_message * len(batch)
            
            if batch_elapsed < expected_time:
                await asyncio.sleep(expected_time - batch_elapsed)
            
            # Progress logging
            if (i + batch_size) % 10000 == 0 or i == 0:
                throughput = self.metrics.get_throughput()
                logger.info(
                    f"Progress: {i + batch_size}/{len(messages)} | "
                    f"Throughput: {throughput:,.0f} msg/s | "
                    f"Clients: {self.metrics.clients_connected}"
                )
        
        # Final metrics
        final_throughput = self.metrics.get_throughput()
        logger.info(f"Stream complete!")
        logger.info(f"Final throughput: {final_throughput:,.0f} msg/s")
        logger.info(f"Total messages: {self.metrics.messages_sent:,}")
        logger.info(f"Errors: {self.metrics.errors}")
    
    async def start(
        self, 
        messages: List[Dict], 
        callback: Optional[Callable] = None
    ):
        """Start TCP server and stream messages"""
        self.running = True
        
        # Start TCP server
        server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port
        )
        
        addr = f"{self.host}:{self.port}"
        logger.info(f"TCP Server listening on {addr}")
        
        # Stream messages
        stream_task = asyncio.create_task(
            self.stream_messages(messages, callback)
        )
        
        try:
            async with server:
                await stream_task
                # FIX: Keep server alive indefinitely after streaming
                logger.info("Stream finished. Server remaining active. Press Ctrl+C to stop.")
                # This awaitable never completes, keeping the server listening
                await asyncio.Future() 
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            self.running = False
            server.close()
            await server.wait_closed()
    
    def get_metrics(self) -> Dict:
        """Get current streaming metrics"""
        return self.metrics.to_dict()


class TCPClient:
    """
    Simple TCP client for testing
    File: src/my_package/tcp_client.py
    """
    
    def __init__(self, host: str = "localhost", port: int = 9999):
        self.host = host
        self.port = port
        self.messages_received = 0
    
    async def connect_and_receive(self, duration: int = 10):
        """Connect and receive messages"""
        logger.info(f"Connecting to {self.host}:{self.port}")
        
        # FIX 1.1: Initialize writer and reader to None outside the try block
        reader, writer = None, None
        start_time = time.time() # Initialize start_time here for safe metrics calculation
        
        try:
            reader, writer = await asyncio.open_connection(self.host, self.port)
            logger.info("Connected!")
            
            while time.time() - start_time < duration:
                # Read length prefix
                length_data = await reader.readexactly(4)
                message_length = struct.unpack('!I', length_data)[0]
                
                # Read message
                message_data = await reader.readexactly(message_length)
                message = json.loads(message_data.decode('utf-8'))
                
                self.messages_received += 1
                
                # Log progress
                if self.messages_received % 1000 == 0:
                    elapsed = time.time() - start_time
                    rate = self.messages_received / elapsed
                    logger.info(f"Received {self.messages_received:,} messages | Rate: {rate:,.0f} msg/s")
        
        except asyncio.IncompleteReadError:
            logger.info("Stream ended")
        except Exception as e:
            logger.error(f"Client error: {e}")
        finally:
            # FIX 1.2: Check if writer was successfully assigned before trying to close it
            if writer:
                writer.close()
                await writer.wait_closed()
            
            elapsed = time.time() - start_time
            rate = self.messages_received / elapsed if elapsed > 0 else 0
            logger.info(f"Final: {self.messages_received:,} messages in {elapsed:.2f}s ({rate:,.0f} msg/s)")


# Example integration with your existing code
async def run_streaming_server_example():
    """
    Example: How to integrate with your existing order book
    """
    from my_package.orderbook import OrderBook  # Your existing order book
    
    # Create order book instance
    order_book = OrderBook()
    
    # Load MBO data
    reader = MBOFileReader("data/mbo_data.csv")
    messages = reader.load()
    
    # Define callback to update your order book
    def update_order_book(message: Dict):
        """Apply message to order book"""
        try:
            # Convert to your format if needed
            event = {
                "symbol": message["symbol"],
                "side": message["side"],
                "price": message["price"],
                "size": message["size"]
            }
            
            # Apply to your order book
            order_book.apply(event)
            
        except Exception as e:
            logger.error(f"Failed to apply message: {e}")
    
    # Create and start server
    server = TCPStreamServer(
        host="0.0.0.0",
        port=9999,
        target_rate=100000  # 100k messages/second
    )
    
    # Start streaming with order book updates
    await server.start(messages, callback=update_order_book)
    
    # This part only runs after Ctrl+C is pressed
    # Find a symbol that was actually processed (e.g., AAPL if it's in your file)
    processed_symbols = order_book.bids.keys() | order_book.asks.keys()
    test_symbol = next(iter(processed_symbols), "TEST")
    
    output = {
        "symbol": test_symbol,
        "bids": order_book.get_bids(test_symbol),
        "asks": order_book.get_asks(test_symbol),
        "timestamp": int(time.time() * 1000)
    }
    
    # NOTE: Ensure the 'output/' directory exists if this code runs successfully
    # with open("output/order_book.json", "w") as f:
    #     json.dump(output, f, indent=2)
    
    # logger.info("Order book saved to output/order_book.json")


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run example
    asyncio.run(run_streaming_server_example())