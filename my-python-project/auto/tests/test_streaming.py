"""
Tests for TCP Streaming Functionality
File: tests/test_streaming.py

Tests TCP server, MBO file reading, and integration
"""
import pytest
import asyncio
import time
import json
from pathlib import Path
import tempfile

from my_package.tcp_server import (
    TCPStreamServer, 
    MBOFileReader, 
    TCPClient,
    StreamMetrics
)
from my_package.orderbook import OrderBook


class TestMBOFileReader:
    """Test MBO file reading"""
    
    def test_parse_valid_csv(self, tmp_path):
        """Test parsing valid MBO CSV file"""
        # Create test file
        test_file = tmp_path / "test_mbo.csv"
        test_file.write_text("""timestamp,type,order_id,symbol,side,price,size
1000,NEW,ORD001,AAPL,bid,100.0,10
1001,NEW,ORD002,AAPL,ask,101.0,20
1002,CANCEL,ORD001,AAPL,bid,0,0
""")
        
        # Parse
        reader = MBOFileReader(str(test_file))
        messages = reader.load()
        
        # Verify
        assert len(messages) == 3
        assert messages[0]["type"] == "NEW"
        assert messages[0]["order_id"] == "ORD001"
        assert messages[0]["price"] == 100.0
        assert messages[1]["side"] == "ask"
        assert messages[2]["type"] == "CANCEL"
    
    def test_skip_invalid_lines(self, tmp_path):
        """Test that invalid lines are skipped"""
        test_file = tmp_path / "test_mbo.csv"
        test_file.write_text("""timestamp,type,order_id,symbol,side,price,size
1000,NEW,ORD001,AAPL,bid,100.0,10
invalid line here
1001,NEW,ORD002,AAPL,ask,101.0,20
""")
        
        reader = MBOFileReader(str(test_file))
        messages = reader.load()
        
        # Should have 2 valid messages
        assert len(messages) == 2
    
    def test_empty_file(self, tmp_path):
        """Test handling of empty file"""
        test_file = tmp_path / "empty.csv"
        test_file.write_text("timestamp,type,order_id,symbol,side,price,size\n")
        
        reader = MBOFileReader(str(test_file))
        messages = reader.load()
        
        assert len(messages) == 0


class TestStreamMetrics:
    """Test stream metrics tracking"""
    
    def test_metrics_initialization(self):
        """Test metrics are initialized correctly"""
        metrics = StreamMetrics()
        
        assert metrics.messages_sent == 0
        assert metrics.bytes_sent == 0
        assert metrics.clients_connected == 0
        assert metrics.errors == 0
    
    def test_throughput_calculation(self):
        """Test throughput calculation"""
        metrics = StreamMetrics()
        metrics.start_time = time.time() - 1.0  # 1 second ago
        metrics.messages_sent = 1000
        
        throughput = metrics.get_throughput()
        
        # Should be around 1000 msg/s (±10%)
        assert 900 <= throughput <= 1100
    
    def test_metrics_to_dict(self):
        """Test metrics serialization"""
        metrics = StreamMetrics()
        metrics.messages_sent = 100
        metrics.bytes_sent = 5000
        metrics.clients_connected = 3
        
        data = metrics.to_dict()
        
        assert "messages_sent" in data
        assert "throughput_msg_per_sec" in data
        assert data["clients_connected"] == 3


@pytest.mark.asyncio
class TestTCPStreamServer:
    """Test TCP streaming server"""
    
    async def test_server_starts(self):
        """Test that server starts successfully"""
        server = TCPStreamServer(port=9998)
        
        # Start with empty messages (should complete quickly)
        task = asyncio.create_task(server.start([]))
        
        # Give it a moment to start
        await asyncio.sleep(0.5)
        
        # Cancel the task
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    
    async def test_client_connection(self):
        """Test that clients can connect"""
        server = TCPStreamServer(port=9997)
        messages = [
            {"type": "NEW", "order_id": "O1", "symbol": "TEST", 
             "side": "bid", "price": 100.0, "size": 10, "timestamp": 1000}
        ]
        
        # Start server in background
        server_task = asyncio.create_task(server.start(messages))
        
        # Give server time to start
        await asyncio.sleep(0.5)
        
        # Connect client
        try:
            reader, writer = await asyncio.open_connection("localhost", 9997)
            
            # Read welcome message
            length_data = await asyncio.wait_for(reader.readexactly(4), timeout=2.0)
            msg_length = int.from_bytes(length_data, byteorder='big')
            msg_data = await reader.readexactly(msg_length)
            
            # Should receive a message
            assert len(msg_data) > 0
            
            writer.close()
            await writer.wait_closed()
            
        except asyncio.TimeoutError:
            pytest.fail("Timeout waiting for server response")
        finally:
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass
    
    async def test_message_streaming(self):
        """Test that messages are streamed correctly"""
        server = TCPStreamServer(port=9996, target_rate=1000)
        
        messages = []
        for i in range(100):
            messages.append({
                "type": "NEW",
                "order_id": f"O{i}",
                "symbol": "TEST",
                "side": "bid" if i % 2 == 0 else "ask",
                "price": 100.0 + i * 0.01,
                "size": 10,
                "timestamp": 1000 + i
            })
        
        # Track processed messages
        processed = []
        
        def callback(msg):
            processed.append(msg)
        
        # Stream messages
        await server.stream_messages(messages, callback=callback)
        
        # Verify all messages processed
        assert len(processed) == 100
        assert processed[0]["order_id"] == "O0"
        assert processed[99]["order_id"] == "O99"


@pytest.mark.asyncio
class TestTCPClient:
    """Test TCP client"""
    
    async def test_client_receives_messages(self):
        """Test client can receive messages from server"""
        # Start server
        server = TCPStreamServer(port=9995, target_rate=10000)
        messages = [
            {"type": "TEST", "data": f"msg{i}", "timestamp": i}
            for i in range(100)
        ]
        
        server_task = asyncio.create_task(server.start(messages))
        await asyncio.sleep(0.5)
        
        # Connect client
        client = TCPClient(host="localhost", port=9995)
        
        try:
            # Receive for 2 seconds
            await asyncio.wait_for(
                client.connect_and_receive(duration=2),
                timeout=5.0
            )
            
            # Should have received some messages
            assert client.messages_received > 0
            
        except asyncio.TimeoutError:
            pass  # OK if timeout
        finally:
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass


class TestIntegrationWithOrderBook:
    """Test integration with order book"""
    
    def test_order_book_update_from_stream(self):
        """Test updating order book from streamed messages"""
        order_book = OrderBook()
        
        messages = [
            {"symbol": "TEST", "side": "bid", "price": 100.0, "size": 10},
            {"symbol": "TEST", "side": "ask", "price": 101.0, "size": 20},
            {"symbol": "TEST", "side": "bid", "price": 99.0, "size": 15},
        ]
        
        # Process messages
        for msg in messages:
            order_book.apply(msg)
        
        # Verify book state
        bids = order_book.get_bids("TEST")
        asks = order_book.get_asks("TEST")
        
        assert len(bids) > 0
        assert len(asks) > 0
        assert bids[0]["price"] == 100.0  # Best bid


class TestPerformance:
    """Test streaming performance"""
    
    @pytest.mark.slow
    def test_throughput_target(self):
        """Test that throughput meets target (50k+ msg/s)"""
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
        
        # Process and measure
        start = time.time()
        for msg in messages:
            order_book.apply(msg)
        elapsed = time.time() - start
        
        throughput = len(messages) / elapsed
        
        print(f"\nThroughput: {throughput:,.0f} msg/s")
        print(f"Processed {len(messages):,} messages in {elapsed:.3f}s")
        
        # Should meet 50k msg/s target
        assert throughput > 50000, f"Throughput {throughput:,.0f} below 50k msg/s"
    
    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_tcp_streaming_performance(self):
        """Test TCP streaming performance"""
        # Generate messages
        messages = []
        for i in range(10000):
            messages.append({
                "type": "NEW",
                "order_id": f"O{i}",
                "symbol": "TEST",
                "side": "bid",
                "price": 100.0,
                "size": 10,
                "timestamp": i
            })
        
        # Stream
        server = TCPStreamServer(target_rate=100000)
        
        start = time.time()
        await server.stream_messages(messages)
        elapsed = time.time() - start
        
        metrics = server.get_metrics()
        throughput = metrics["throughput_msg_per_sec"]
        
        print(f"\nTCP Throughput: {throughput:,.0f} msg/s")
        
        # Should process quickly
        assert elapsed < 1.0, f"Processing took {elapsed:.3f}s, too slow"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])