# File: src/my_package/metrics.py
"""
Thread-safe utilities for recording and calculating system metrics,
including message throughput, errors, uptime, and processing latency.
"""
import threading
import time
from statistics import mean
from typing import List, Dict, Any, Optional

# --- Global State ---
_lock = threading.Lock()
_start_time = time.time()
_messages = 0
_errors = 0
_latencies: List[float] = []  # milliseconds

# --- Recording Functions ---

def record_ingest(count: int = 1) -> None:
    """Record the ingestion of one or more messages."""
    global _messages
    with _lock:
        _messages += count


def record_message(count: int = 1) -> None:
    """Compatibility alias used by server.py (historical name)."""
    record_ingest(count)


def record_error(count: int = 1) -> None:
    """Record one or more processing errors."""
    global _errors
    with _lock:
        _errors += count


def record_latency(ms: float) -> None:
    """Record a single latency measurement in milliseconds."""
    with _lock:
        _latencies.append(float(ms))

# --- Calculation Utilities ---

def _percentile(sorted_list: List[float], q: float) -> Optional[float]:
    """Calculate the q-th percentile of a sorted list."""
    if not sorted_list:
        return None
        
    n = len(sorted_list)
    # Convert q (e.g., 0.95) to an index (0-based)
    k_float = (n - 1) * q
    
    # Simple interpolation: k is between index f and index c
    f = int(k_float) # floor index
    
    if f >= n - 1:
        return sorted_list[-1]
        
    c = f + 1 # ceiling index
    
    # Fractional part of the index
    frac = k_float - f
    
    # Linear interpolation
    p_value = sorted_list[f] + frac * (sorted_list[c] - sorted_list[f])
    return p_value


# --- Metrics Snapshot ---

def get_metrics() -> Dict[str, Any]:
    """Return aggregated metrics snapshot."""
    with _lock:
        # Take thread-safe snapshot of mutable variables
        msgs = _messages
        errs = _errors
        lat_copy = list(_latencies)

    uptime = time.time() - _start_time
    throughput = msgs / uptime if uptime > 0 else 0.0

    lat_stats: Dict[str, Any] = {}
    if lat_copy:
        lat_sorted = sorted(lat_copy)
        
        # Calculate size for mean/min/max
        lat_stats = {
            "count": len(lat_sorted),
            "min_ms": min(lat_sorted),
            "max_ms": max(lat_sorted),
            "mean_ms": mean(lat_sorted),
            # Calculate percentiles
            "p50_ms": _percentile(lat_sorted, 0.50),
            "p95_ms": _percentile(lat_sorted, 0.95),
            "p99_ms": _percentile(lat_sorted, 0.99),
        }

    return {
        "messages_total": msgs,
        "errors_total": errs,
        "throughput_msg_per_sec": throughput,
        "uptime_sec": uptime,
        "latency": lat_stats,
    }


def get_stats() -> Dict[str, Any]:
    """Compatibility alias (historical name used by server)."""
    return get_metrics()


def reset() -> None:
    """Reset all metrics (useful for tests)."""
    global _start_time, _messages, _errors, _latencies
    with _lock:
        _start_time = time.time()
        _messages = 0
        _errors = 0
        _latencies = []