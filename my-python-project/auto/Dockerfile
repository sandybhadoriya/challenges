FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY auto/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY auto/src ./src
COPY auto/tests ./tests

# Expose ports
EXPOSE 8000 9999

# Health check
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health').read()"

# Run server
ENV PYTHONPATH=/app/src
CMD ["python", "-m", "my_package.cli", "--option", "serve"]