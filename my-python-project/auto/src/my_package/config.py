import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class Config:
    """Configuration from environment variables."""
    host: str = os.getenv("HOST", "0.0.0.0")
    port: int = int(os.getenv("PORT", "8000"))
    db_path: str = os.getenv("ORDERBOOK_DB_URL", "sqlite:///./orderbook.db")
    log_level: str = os.getenv("LOG_LEVEL", "info")
    environment: str = os.getenv("ENVIRONMENT", "dev")
    max_workers: int = int(os.getenv("MAX_WORKERS", "4"))
    enable_metrics: bool = os.getenv("ENABLE_METRICS", "true").lower() == "true"
    enable_tracing: bool = os.getenv("ENABLE_TRACING", "false").lower() == "true"

    @classmethod
    def from_env(cls):
        return cls()


config = Config.from_env()

# Module-level compatibility aliases (server expects config.db_path, etc.)
host = config.host
port = config.port
db_path = config.db_path
log_level = config.log_level
environment = config.environment
max_workers = config.max_workers
enable_metrics = config.enable_metrics
enable_tracing = config.enable_tracing