FROM python:3.11-slim

WORKDIR /app

# Install system CA certificates so custom CAs can be trusted inside containers.
# Keep image small by removing apt lists afterward.
RUN apt-get update && \
	apt-get install -y --no-install-recommends ca-certificates && \
	rm -rf /var/lib/apt/lists/*

# Copy Python requirements and install before source copy for better caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code (including optional `certs/` directory for custom CAs).
COPY . .

# If custom certs (.crt) are provided under /app/certs, add them to the system trust store.
RUN if [ -d /app/certs ] && ls /app/certs/*.crt >/dev/null 2>&1; then \
	  cp /app/certs/*.crt /usr/local/share/ca-certificates/ 2>/dev/null || true; \
	  update-ca-certificates || true; \
	fi

# Default command — local demo friendly (weather JSON -> Kafka)
CMD ["python", "-u", "ingest_weather.py"]
