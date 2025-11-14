from typing import Optional
from sqlalchemy import Table, Column, Integer, String, Float, MetaData, create_engine, insert
from sqlalchemy.engine import Engine
import logging

logger = logging.getLogger("my_package.db")

metadata = MetaData()

events_table = Table(
    "events",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("symbol", String, nullable=False),
    Column("side", String, nullable=False),
    Column("price", Float, nullable=False),
    Column("size", Integer, nullable=False),
)

_engine: Optional[Engine] = None


def init_db(path: str = "sqlite:///./orderbook.db") -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(path, future=True, echo=False)
        metadata.create_all(_engine)
        logger.info(f"Database initialized: {path}")
    return _engine


def persist_event(ev: dict) -> None:
    if _engine is None:
        init_db()
    try:
        with _engine.connect() as conn:
            conn.execute(
                insert(events_table).values(
                    symbol=ev["symbol"],
                    side=ev["side"],
                    price=float(ev["price"]),
                    size=int(ev["size"]),
                )
            )
            conn.commit()
    except Exception as ex:
        logger.error(f"Failed to persist event: {ex}")
        raise