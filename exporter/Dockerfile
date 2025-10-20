# exporter/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system deps for MLflow's HTTP client if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY exporter.py .

ENV PYTHONUNBUFFERED=1

EXPOSE 8000

CMD ["python", "exporter.py"]
