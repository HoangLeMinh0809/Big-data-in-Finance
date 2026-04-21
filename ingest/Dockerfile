FROM python:3.11-slim

WORKDIR /app

# Cài đặt dependencies trước để tận dụng Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Default command — local demo friendly (weather JSON -> Kafka)
CMD ["python", "-u", "ingest_weather.py"]
