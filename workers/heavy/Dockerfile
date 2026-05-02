FROM python:3.11-slim

WORKDIR /app

COPY workers/heavy/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Flows baked in — dev override with volume mount for hot reload
COPY flows/ ./flows/

CMD ["prefect", "worker", "start", "--pool", "heavy-pool"]
