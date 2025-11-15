# File: src/my_package/config.py
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class Config:
    """Configuration loaded from environment variables."""
    # Network settings
    host: str = os.getenv("HOST", "0.0.0.0")
    port: int = int(os.getenv("PORT", "8000"))
    
    # Database connection (Placeholder for persistence, though not implemented yet)
    db_path: str = os.getenv("ORDERBOOK_DB_URL", "sqlite:///./orderbook.db")
    
    # System settings
    log_level: str = os.getenv("LOG_LEVEL", "info").upper() # Normalize to uppercase
    environment: str = os.getenv("ENVIRONMENT", "dev")
    max_workers: int = int(os.getenv("MAX_WORKERS", "4"))
    
    # Feature flags
    enable_metrics: bool = os.getenv("ENABLE_METRICS", "true").lower() == "true"
    enable_tracing: bool = os.getenv("ENABLE_TRACING", "false").lower() == "true"

    @classmethod
    def from_env(cls):
        """Initializes configuration from environment variables."""
        return cls()


config = Config.from_env()

# Module-level compatibility aliases (if needed, but direct use of `config` is better practice)
host = config.host
port = config.port
db_path = config.db_path
log_level = config.log_level
environment = config.environment
max_workers = config.max_workers
enable_metrics = config.enable_metrics
enable_tracing = config.enable_tracing