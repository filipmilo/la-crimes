FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the stream processor
COPY stream_processor.py .

# Set environment variables
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV ELASTICSEARCH_HOST=elasticsearch:9200

CMD ["python", "stream_processor.py"]