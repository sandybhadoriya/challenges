from locust import HttpUser, task, between
import random
import json

SYMBOLS = ["AAPL", "GOOG", "TSLA", "MSFT", "AMZN"]

class IngestUser(HttpUser):
    wait_time = between(0.01, 0.2)

    @task(6)
    def post_ingest(self):
        payload = {
            "symbol": random.choice(SYMBOLS),
            "side": random.choice(["bid", "ask"]),
            "price": round(random.uniform(1.0, 2000.0), 2),
            "size": random.randint(1, 500),
        }
        self.client.post("/ingest", json=payload)

    @task(2)
    def get_book(self):
        sym = random.choice(SYMBOLS)
        self.client.get(f"/book/{sym}?depth=5")

    @task(1)
    def get_metrics(self):
        self.client.get("/metrics")