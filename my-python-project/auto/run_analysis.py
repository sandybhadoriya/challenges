# File: run_analysis.py
import json
import time
import os
import logging
# Assuming my_package is accessible via PYTHONPATH=$PYTHONPATH:./src
from my_package.orderbook import OrderBook
from my_package.tcp_server import MBOFileReader # Reuses the MBO reader logic

# --- Configuration ---
MBO_FILE_PATH = "data/mbo_data.csv"
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True) # Ensure the output directory exists
OUTPUT_FILENAME = f"{OUTPUT_DIR}/reconstructed_orderbook_{int(time.time())}.json"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def run_reconstruction_and_save():
    """Main function to reconstruct the book and generate the final JSON file."""
    
    if not os.path.exists(MBO_FILE_PATH):
        logging.error(f"Error: MBO data file not found at '{MBO_FILE_PATH}'")
        return

    reader = MBOFileReader(MBO_FILE_PATH)
    try:
        messages = reader.load()
    except Exception as e:
        logging.error(f"Failed to load MBO data: {e}")
        return
        
    logging.info(f"Loaded {len(messages)} messages.")
    
    order_book = OrderBook()
    logging.info("Starting Order Book reconstruction...")
    start_time = time.time()
    
    for i, msg in enumerate(messages):
        try:
            # Note: order_book.apply expects a dictionary compatible with MBOFileReader's output
            order_book.apply(msg) 
            if (i + 1) % 100000 == 0:
                logging.info(f"Processed {i + 1} messages...")
        except Exception as e:
            logging.warning(f"Skipping message {i} ({msg.get('symbol', 'N/A')}): {e}")
            
    elapsed_time = time.time() - start_time
    throughput = len(messages) / elapsed_time if elapsed_time > 0 else 0
    
    logging.info(f"Reconstruction finished in {elapsed_time:.3f}s.")
    logging.info(f"Achieved throughput: {throughput:,.0f} msg/s.")
    logging.info(f"P99 Latency: {order_book.get_p99_latency():.3f} ms")
    
    # Generate and Save JSON Deliverable
    final_book_state = order_book.get_full_book_state()
    
    try:
        with open(OUTPUT_FILENAME, 'w') as f:
            json.dump(final_book_state, f, indent=4)
        
        logging.info(f"✅ Final order book state saved to: {OUTPUT_FILENAME}")
        
    except Exception as e:
        logging.error(f"❌ Error saving JSON file: {e}")

if __name__ == "__main__":
    run_reconstruction_and_save()