# File: run_client.py
import asyncio
import logging
# Ensure this import path is correct for your project structure
from my_package.tcp_server import TCPClient 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

async def main():
    client = TCPClient(host="localhost", port=9999)
    # Set duration high to keep client alive until server is shut down or stream ends.
    await client.connect_and_receive(duration=600)

if __name__ == "__main__":
    asyncio.run(main())