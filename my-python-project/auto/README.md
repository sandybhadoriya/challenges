# My Python Project - Order Book demo

Quick start (Linux):
1. cd /home/santosh/my-python-project/auto
2. python -m venv .venv
3. source .venv/bin/activate
4. pip install -r requirements.txt

Run server:
PYTHONPATH=./src python -m my_package.cli --option serve

API:
- POST /ingest  { "symbol":"TST","side":"bid","price":100.0,"size":10 }
- GET  /book/TST
- GET  /health

Run tests:
PYTHONPATH=./src pytest