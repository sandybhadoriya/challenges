import threading
import time
from statistics import mean


_lock = threading.Lock()
_start_time = time.time()
_messages = 0
_errors = 0
_latencies = []  # milliseconds


def record_ingest(count: int = 1) -> None:
    global _messages
    with _lock:
        _messages += count


def record_message(count: int = 1) -> None:
    """Compatibility alias used by server.py (historical name)."""
    record_ingest(count)


def record_error(count: int = 1) -> None:
    global _errors
    with _lock:
        _errors += count


def record_latency(ms: float) -> None:
    """Record a single latency measurement in milliseconds."""
    with _lock:
        _latencies.append(float(ms))


def _percentile(sorted_list, q: float):
    if not sorted_list:
        return None
    k = (len(sorted_list) - 1) * q
    f = int(k)
    c = f + 1
    if c >= len(sorted_list):
        return sorted_list[-1]
    d0 = sorted_list[f] * (c - k)
    d1 = sorted_list[c] * (k - f)
    return d0 + d1


def get_metrics() -> dict:
    """Return aggregated metrics snapshot."""
    with _lock:
        msgs = _messages
        errs = _errors
        lat_copy = list(_latencies)

    uptime = time.time() - _start_time
    throughput = msgs / uptime if uptime > 0 else 0.0

    lat_stats = {}
    if lat_copy:
        lat_sorted = sorted(lat_copy)
        lat_stats = {
            "count": len(lat_sorted),
            "min_ms": min(lat_sorted),
            "max_ms": max(lat_sorted),
            "mean_ms": mean(lat_sorted),
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


def get_stats() -> dict:
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