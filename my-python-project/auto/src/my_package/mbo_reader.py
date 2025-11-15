"""
MBO File Reader
File: src/my_package/mbo_reader.py

Reads and parses Market-By-Order (MBO) data from CSV files
"""
import logging
from typing import List, Dict, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class MBOFileReader:
    """
    Read and parse MBO data from CSV files
    
    Expected CSV format:
    timestamp,type,order_id,symbol,side,price,size
    
    Where:
    - timestamp: Unix timestamp in milliseconds
    - type: NEW, CANCEL, MODIFY, or EXECUTE
    - order_id: Unique order identifier
    - symbol: Trading symbol (e.g., AAPL, MSFT)
    - side: 'bid' or 'ask'
    - price: Order price (float)
    - size: Order size/quantity (int)
    """
    
    def __init__(self, filepath: str):
        """
        Initialize MBO file reader
        
        Args:
            filepath: Path to MBO CSV file
        """
        self.filepath = filepath
        self.messages = []
        self.errors = 0
        
    def load(self) -> List[Dict]:
        """
        Load and parse MBO file
        
        Returns:
            List of parsed message dictionaries
            
        Raises:
            FileNotFoundError: If file doesn't exist
        """
        filepath = Path(self.filepath)
        
        if not filepath.exists():
            raise FileNotFoundError(f"MBO file not found: {self.filepath}")
        
        logger.info(f"Loading MBO file: {self.filepath}")
        
        self.messages = []
        self.errors = 0
        
        with open(filepath, 'r') as f:
            # Read and skip header
            header = f.readline().strip()
            
            # Validate header
            expected_headers = ['timestamp', 'type', 'order_id', 'symbol', 'side', 'price', 'size']
            actual_headers = [h.strip().lower() for h in header.split(',')]
            
            if actual_headers != expected_headers:
                logger.warning(
                    f"Unexpected header format. Expected: {expected_headers}, "
                    f"Got: {actual_headers}"
                )
            
            # Parse data lines
            for line_num, line in enumerate(f, start=2):
                line = line.strip()
                
                # Skip empty lines and comments
                if not line or line.startswith('#'):
                    continue
                
                try:
                    message = self._parse_line(line)
                    if message:
                        self.messages.append(message)
                except Exception as e:
                    logger.warning(f"Line {line_num}: Failed to parse - {e}")
                    logger.debug(f"  Line content: {line}")
                    self.errors += 1
        
        logger.info(
            f"Loaded {len(self.messages):,} messages from {self.filepath} "
            f"({self.errors} errors)"
        )
        
        return self.messages
    
    def _parse_line(self, line: str) -> Optional[Dict]:
        """
        Parse a single MBO message line
        
        Args:
            line: CSV line to parse
            
        Returns:
            Parsed message dictionary or None if invalid
        """
        parts = [p.strip() for p in line.split(',')]
        
        # Validate minimum fields
        if len(parts) < 7:
            logger.debug(f"Insufficient fields: {len(parts)} < 7")
            return None
        
        try:
            # Parse fields
            timestamp = int(parts[0])
            msg_type = parts[1].upper()
            order_id = parts[2]
            symbol = parts[3].upper()
            side = parts[4].lower()
            
            # Price and size may be 0 for CANCEL messages
            try:
                price = float(parts[5])
            except (ValueError, IndexError):
                price = 0.0
            
            try:
                size = int(parts[6])
            except (ValueError, IndexError):
                size = 0
            
            # Validate message type
            valid_types = ['NEW', 'CANCEL', 'MODIFY', 'EXECUTE']
            if msg_type not in valid_types:
                logger.debug(f"Invalid message type: {msg_type}")
                return None
            
            # Validate side (convert variations to standard format)
            if side in ['bid', 'buy', 'b']:
                side = 'bid'
            elif side in ['ask', 'sell', 's']:
                side = 'ask'
            else:
                logger.debug(f"Invalid side: {side}")
                return None
            
            # Build message
            message = {
                'timestamp': timestamp,
                'type': msg_type,
                'order_id': order_id,
                'symbol': symbol,
                'side': side,
                'price': price,
                'size': size
            }
            
            # Validate based on message type
            if msg_type == 'NEW':
                if price <= 0 or size <= 0:
                    logger.debug(f"NEW order with invalid price/size: {price}/{size}")
                    return None
            
            return message
            
        except (ValueError, IndexError) as e:
            logger.debug(f"Parse error: {e}")
            return None
    
    def get_messages(self) -> List[Dict]:
        """
        Get all loaded messages
        
        Returns:
            List of message dictionaries
        """
        return self.messages
    
    def get_stats(self) -> Dict:
        """
        Get statistics about loaded data
        
        Returns:
            Dictionary with statistics
        """
        if not self.messages:
            return {
                'total_messages': 0,
                'errors': self.errors,
                'message_types': {},
                'symbols': set(),
                'time_range': (None, None)
            }
        
        # Count message types
        type_counts = {}
        symbols = set()
        
        for msg in self.messages:
            msg_type = msg['type']
            type_counts[msg_type] = type_counts.get(msg_type, 0) + 1
            symbols.add(msg['symbol'])
        
        # Get time range
        timestamps = [msg['timestamp'] for msg in self.messages]
        time_range = (min(timestamps), max(timestamps))
        
        return {
            'total_messages': len(self.messages),
            'errors': self.errors,
            'message_types': type_counts,
            'symbols': symbols,
            'time_range': time_range,
            'duration_ms': time_range[1] - time_range[0] if time_range[0] else 0
        }
    
    def print_stats(self):
        """Print statistics about loaded data"""
        stats = self.get_stats()
        
        print("\n" + "="*60)
        print("MBO File Statistics")
        print("="*60)
        print(f"File: {self.filepath}")
        print(f"Total messages: {stats['total_messages']:,}")
        print(f"Parse errors: {stats['errors']}")
        print()
        print("Message types:")
        for msg_type, count in sorted(stats['message_types'].items()):
            print(f"  {msg_type:10s}: {count:,}")
        print()
        print(f"Symbols: {', '.join(sorted(stats['symbols']))}")
        print(f"Time range: {stats['time_range'][0]} - {stats['time_range'][1]}")
        print(f"Duration: {stats['duration_ms']:,} ms")
        print("="*60 + "\n")


# Utility functions

def read_mbo_file(filepath: str, print_stats: bool = True) -> List[Dict]:
    """
    Convenience function to read MBO file
    
    Args:
        filepath: Path to MBO CSV file
        print_stats: Whether to print statistics
        
    Returns:
        List of parsed messages
    """
    reader = MBOFileReader(filepath)
    messages = reader.load()
    
    if print_stats:
        reader.print_stats()
    
    return messages


def validate_mbo_file(filepath: str) -> bool:
    """
    Validate MBO file format
    
    Args:
        filepath: Path to MBO CSV file
        
    Returns:
        True if valid, False otherwise
    """
    try:
        reader = MBOFileReader(filepath)
        messages = reader.load()
        
        if not messages:
            logger.error("No valid messages found in file")
            return False
        
        stats = reader.get_stats()
        
        # Check for high error rate
        if stats['errors'] > len(messages) * 0.1:  # More than 10% errors
            logger.error(
                f"High error rate: {stats['errors']} errors "
                f"in {len(messages)} messages"
            )
            return False
        
        logger.info("MBO file validation successful")
        return True
        
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        return False


# Example usage
if __name__ == "__main__":
    import sys
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    if len(sys.argv) < 2:
        print("Usage: python mbo_reader.py <mbo_file.csv>")
        sys.exit(1)
    
    filepath = sys.argv[1]
    
    # Read and display stats
    messages = read_mbo_file(filepath, print_stats=True)
    
    # Show first few messages
    print("First 5 messages:")
    for i, msg in enumerate(messages[:5], 1):
        print(f"{i}. {msg}")