from locust import HttpUser, task, between
import random
import time

SYMBOLS = ["AAPL", "GOOG", "TSLA", "MSFT", "AMZN"]
SIDES = ["bid", "ask"]

class IngestUser(HttpUser):
    wait_time = between(0.01, 0.2)

    def _make_mbp_update(self, symbol: str):
        # MBP-style payload: feed + seq + timestamp + list of price level updates
        updates = []
        for _ in range(random.randint(1, 3)):
            updates.append({
                "side": random.choice(SIDES),
                "price": round(random.uniform(1.0, 2000.0), 2),
                "size": random.randint(0, 500),  # allow zero for removals
            })
        payload = {
            "feed": "MBP",
            "symbol": symbol,
            "seq": random.randint(1, 1_000_000),
            "timestamp": time.time(),
            "updates": updates
        }
        return payload

    @task(6)
    def post_ingest(self):
        payload = self._make_mbp_update(random.choice(SYMBOLS))
        # server should accept MBP payload and expand/apply each update
        self.client.post("/ingest", json=payload)

    @task(2)
    def get_book(self):
        sym = random.choice(SYMBOLS)
        self.client.get(f"/book/{sym}?depth=5")

    @task(1)
    def get_metrics(self):
        self.client.get("/metrics")