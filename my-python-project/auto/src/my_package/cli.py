"""
Complete CLI with TCP Streaming Support
File: src/my_package/cli.py

REPLACE your existing cli.py with this complete version

Supports both modes:
1. Stream mode (for Batonics challenge) - NEW
2. Serve mode (your existing FastAPI server) - PRESERVED
"""
import asyncio
import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Optional, Dict

# Import your existing modules
try:
    from my_package.orderbook import OrderBook
    from my_package.api import app  # Your FastAPI app
except ImportError as e:
    print(f"ERROR: Could not import existing modules: {e}")
    print("Make sure you're running with: PYTHONPATH=./src")
    sys.exit(1)

# Import streaming modules (new)
STREAMING_AVAILABLE = False
try:
    from my_package.tcp_server import TCPStreamServer, MBOFileReader
    STREAMING_AVAILABLE = True
except ImportError:
    # Streaming not available yet - that's OK, serve mode will still work
    pass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StreamingMode:
    """
    Handle streaming mode for Batonics challenge
    
    Workflow:
    1. Load MBO data from CSV file
    2. Start TCP server on specified port
    3. Stream messages and update order book
    4. Save final order book state to JSON
    5. Print metrics and summary
    """
    
    def __init__(
        self,
        input_file: str,
        output_file: str,
        port: int = 9999,
        target_rate: int = 100000
    ):
        self.input_file = input_file
        self.output_file = output_file
        self.port = port
        self.target_rate = target_rate
        
        # Initialize order book
        self.order_book = OrderBook()
        
        # Metrics
        self.message_count = 0
        self.errors = 0
        self.start_time = None
    
    def process_message(self, message: dict):
        """
        Process MBO message and update order book
        
        Converts MBO format to your OrderBook format and applies update.
        This is called for each message during streaming.
        """
        try:
            # Convert MBO message to OrderBook event format
            event = {
                "symbol": message.get("symbol", "UNKNOWN"),
                "side": message.get("side", "bid"),  # 'bid' or 'ask'
                "price": float(message.get("price", 0)),
                "size": int(message.get("size", 0))
            }
            
            # Apply to your existing order book
            self.order_book.apply(event)
            self.message_count += 1
            
        except Exception as e:
            logger.error(f"Failed to process message: {e}")
            logger.debug(f"Message was: {message}")
            self.errors += 1
    
    async def run(self):
        """Run streaming mode - main workflow"""
        self.start_time = time.time()
        
        # Print banner
        print("\n" + "="*80)
        print("STREAMING MODE - Batonics Trading Systems Challenge")
        print("="*80)
        print(f"Input file:    {self.input_file}")
        print(f"Output file:   {self.output_file}")
        print(f"TCP Port:      {self.port}")
        print(f"Target Rate:   {self.target_rate:,} msg/s")
        print("="*80 + "\n")
        
        # Step 1: Load MBO data
        logger.info("Step 1: Loading MBO data...")
        try:
            reader = MBOFileReader(self.input_file)
            messages = reader.load()
            
            if not messages:
                logger.error("❌ No messages loaded from file!")
                return
            
            logger.info(f"✓ Loaded {len(messages):,} messages")
            
            # Print file statistics
            stats = reader.get_stats()
            print(f"\nData Statistics:")
            print(f"  Messages: {stats['total_messages']:,}")
            print(f"  Symbols: {', '.join(sorted(stats['symbols']))}")
            print(f"  Types: {', '.join(f'{k}:{v}' for k, v in stats['message_types'].items())}")
            print()
            
        except FileNotFoundError as e:
            logger.error(f"❌ File not found: {e}")
            logger.info("Generate sample data with: python scripts/generate_mbo_data.py")
            return
        except Exception as e:
            logger.error(f"❌ Failed to load MBO data: {e}")
            return
        
        # Step 2: Create TCP server
        logger.info("Step 2: Creating TCP streaming server...")
        server = TCPStreamServer(
            host="0.0.0.0",
            port=self.port,
            target_rate=self.target_rate
        )
        logger.info(f"✓ TCP server configured on port {self.port}")
        
        # Step 3: Start streaming
        logger.info("Step 3: Starting message stream...")
        try:
            await server.start(messages, callback=self.process_message)
            logger.info("✓ Streaming complete")
        except Exception as e:
            logger.error(f"❌ Streaming failed: {e}")
            return
        
        # Step 4: Save order book
        logger.info("Step 4: Saving order book to JSON...")
        self.save_order_book()
        
        # Step 5: Print summary
        self.print_summary(server)
    
    def save_order_book(self):
        """Save order book state to JSON file"""
        output_path = Path(self.output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            # Get all symbols from order book
            # Try different methods to get symbols
            symbols = []
            if hasattr(self.order_book, 'get_symbols'):
                symbols = self.order_book.get_symbols()
            elif hasattr(self.order_book, 'symbols'):
                symbols = list(self.order_book.symbols)
            elif hasattr(self.order_book, '_books'):
                symbols = list(self.order_book._books.keys())
            else:
                # Fallback - try common symbols
                symbols = ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN", "TEST"]
            
            # Build output for each symbol
            output = {}
            symbols_exported = 0
            
            for symbol in symbols:
                try:
                    # Get bids and asks using your OrderBook methods
                    bids = self.order_book.get_bids(symbol)
                    asks = self.order_book.get_asks(symbol)
                    
                    # Only include if we have data
                    if bids or asks:
                        output[symbol] = {
                            "bids": bids[:20] if bids else [],  # Top 20 levels
                            "asks": asks[:20] if asks else [],
                            "timestamp": int(time.time() * 1000)
                        }
                        symbols_exported += 1
                    
                except Exception as e:
                    logger.debug(f"Could not export symbol {symbol}: {e}")
                    continue
            
            # Add metadata
            output["_metadata"] = {
                "messages_processed": self.message_count,
                "errors": self.errors,
                "symbols_count": symbols_exported,
                "generated_at": int(time.time() * 1000),
                "source_file": self.input_file
            }
            
            # Save to file
            with open(output_path, 'w') as f:
                json.dump(output, f, indent=2)
            
            file_size = output_path.stat().st_size
            logger.info(f"✓ Order book saved to {output_path}")
            logger.info(f"  File size: {file_size:,} bytes")
            logger.info(f"  Symbols exported: {symbols_exported}")
            
        except Exception as e:
            logger.error(f"❌ Failed to save order book: {e}")
            import traceback
            traceback.print_exc()
    
    def print_summary(self, server):
        """Print execution summary with metrics"""
        elapsed = time.time() - self.start_time
        metrics = server.get_metrics()
        
        print("\n" + "="*80)
        print("STREAMING COMPLETE - SUMMARY")
        print("="*80)
        print(f"Execution time:       {elapsed:.2f}s")
        print(f"Messages processed:   {self.message_count:,}")
        print(f"Messages sent:        {metrics['messages_sent']:,}")
        print(f"Throughput:           {metrics['throughput_msg_per_sec']:,.0f} msg/s")
        print(f"Clients served:       {metrics.get('clients_connected', 0)}")
        print(f"Errors:               {self.errors}")
        print("="*80)
        
        # Verify correctness if method exists
        try:
            if hasattr(self.order_book, 'verify_correctness'):
                violations = self.order_book.verify_correctness()
                if violations:
                    print(f"⚠  WARNING: {len(violations)} correctness violations detected!")
                    for v in violations[:5]:  # Show first 5
                        print(f"   - {v}")
                else:
                    print("✓  Order book correctness verified - no violations")
            else:
                print("ℹ  Correctness verification not available (add verify_correctness method)")
        except Exception as e:
            print(f"⚠  Could not verify correctness: {e}")
        
        print("="*80 + "\n")
        
        # Check if target met
        throughput = metrics['throughput_msg_per_sec']
        if throughput >= 50000:
            print(f"🎉 SUCCESS: Throughput {throughput:,.0f} msg/s meets 50k requirement!")
        else:
            print(f"⚠  Throughput {throughput:,.0f} msg/s below 50k target")
        print()


class ServeMode:
    """
    Handle FastAPI server mode (your existing functionality)
    This preserves your original API server behavior
    """
    
    def __init__(self, host: str = "0.0.0.0", port: int = 8000):
        self.host = host
        self.port = port
    
    def run(self):
        """Run FastAPI server"""
        try:
            import uvicorn
        except ImportError:
            logger.error("❌ uvicorn not installed")
            logger.error("Install with: pip install uvicorn")
            return
        
        print("\n" + "="*80)
        print("API SERVER MODE")
        print("="*80)
        print(f"Starting FastAPI server on http://{self.host}:{self.port}")
        print()
        print("API Endpoints:")
        print("  POST   /ingest           - Ingest order events")
        print("  GET    /book/{symbol}    - Get order book")
        print("  GET    /metrics          - Get metrics")
        print("  GET    /verify/{symbol}  - Verify correctness")
        print("  GET    /health           - Health check")
        print()
        print("Press Ctrl+C to stop")
        print("="*80 + "\n")
        
        try:
            uvicorn.run(
                app,
                host=self.host,
                port=self.port,
                log_level="info"
            )
        except Exception as e:
            logger.error(f"❌ Server failed: {e}")


def main():
    """Main CLI entry point"""
    
    parser = argparse.ArgumentParser(
        description="Trading System - Order Book Processing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Streaming mode (for Batonics challenge)
  PYTHONPATH=./src python -m my_package.cli --mode stream \\
      --input data/mbo_data.csv \\
      --output output/order_book.json
  
  # API server mode (your existing functionality)
  PYTHONPATH=./src python -m my_package.cli --mode serve --port 8000
  
  # Streaming with custom settings
  PYTHONPATH=./src python -m my_package.cli --mode stream \\
      --input data/mbo_large.csv \\
      --rate 200000 \\
      --tcp-port 9999

  # Backwards compatible (old way)
  PYTHONPATH=./src python -m my_package.cli --option serve
        """
    )
    
    # Mode selection
    parser.add_argument(
        "--mode",
        choices=["stream", "serve"],
        help="Operation mode: 'stream' for TCP streaming, 'serve' for API server (default: serve)"
    )
    
    # Streaming mode options
    parser.add_argument(
        "--input",
        type=str,
        default="data/mbo_data.csv",
        help="Input MBO data file (CSV format) [stream mode]"
    )
    
    parser.add_argument(
        "--output",
        type=str,
        default="output/order_book.json",
        help="Output JSON file [stream mode]"
    )
    
    parser.add_argument(
        "--tcp-port",
        type=int,
        default=9999,
        help="TCP port for streaming (default: 9999) [stream mode]"
    )
    
    parser.add_argument(
        "--rate",
        type=int,
        default=100000,
        help="Target throughput in messages/second (default: 100000) [stream mode]"
    )
    
    # API server mode options
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="Host address (default: 0.0.0.0) [serve mode]"
    )
    
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="API port (default: 8000) [serve mode]"
    )
    
    # Backwards compatibility with your existing --option flag
    parser.add_argument(
        "--option",
        type=str,
        choices=["serve", "stream"],
        help="[DEPRECATED] Use --mode instead"
    )
    
    args = parser.parse_args()
    
    # Handle backwards compatibility
    if args.option:
        if not args.mode:
            args.mode = args.option
            logger.info(f"Using --option={args.option} (deprecated, use --mode instead)")
    
    # Default to serve mode if nothing specified
    if not args.mode:
        args.mode = "serve"
        logger.info("No mode specified, defaulting to 'serve' mode")
    
    try:
        if args.mode == "stream":
            # Check if streaming is available
            if not STREAMING_AVAILABLE:
                print("\n" + "="*80)
                print("ERROR: Streaming mode not available")
                print("="*80)
                print("You need to create these files first:")
                print("  1. src/my_package/tcp_server.py")
                print("  2. src/my_package/mbo_reader.py")
                print()
                print("See the implementation guide for complete code.")
                print("="*80 + "\n")
                sys.exit(1)
            
            # Run streaming mode
            logger.info("🚀 Starting in STREAM mode")
            streaming = StreamingMode(
                input_file=args.input,
                output_file=args.output,
                port=args.tcp_port,
                target_rate=args.rate
            )
            asyncio.run(streaming.run())
            
        elif args.mode == "serve":
            # Run API server mode
            logger.info("🚀 Starting in SERVE mode")
            server = ServeMode(host=args.host, port=args.port)
            server.run()
            
    except KeyboardInterrupt:
        print("\n")
        logger.info("👋 Shutdown requested by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()