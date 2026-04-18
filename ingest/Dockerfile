FROM python:3.9-slim

WORKDIR /app

# Cài đặt dependencies trước để tận dụng Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Default command — chạy ingest service
CMD ["python", "-u", "ingest.py"]
