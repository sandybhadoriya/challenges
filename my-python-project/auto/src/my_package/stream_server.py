"""TCP server for streaming order book messages."""
import asyncio
import logging
from my_package.stream import OrderBookReconstructor

logger = logging.getLogger("my_package.stream_server")


class StreamServer:
    """Async TCP server accepting MBO messages."""
    
    def __init__(self, host: str = "0.0.0.0", port: int = 9999):
        self.host = host
        self.port = port
        self.reconstructor = OrderBookReconstructor()
        self.clients = set()
    
    async def handle_client(self, reader, writer):
        """Handle single client connection."""
        addr = writer.get_extra_info('peername')
        logger.info(f"Client connected: {addr}")
        self.clients.add(writer)
        
        try:
            while True:
                data = await reader.readline()
                if not data:
                    break
                
                line = data.decode('utf-8')
                try:
                    self.reconstructor.apply(line)
                except Exception as ex:
                    logger.error(f"Error processing message: {ex}")
                    await writer.write(f"ERROR: {ex}\n".encode())
                    await writer.drain()
        
        except asyncio.CancelledError:
            logger.info(f"Client disconnected: {addr}")
        finally:
            self.clients.discard(writer)
            writer.close()
            await writer.wait_closed()
    
    async def start(self):
        """Start TCP server."""
        server = await asyncio.start_server(
            self.handle_client, self.host, self.port
        )
        logger.info(f"Stream server listening on {self.host}:{self.port}")
        async with server:
            await server.serve_forever()