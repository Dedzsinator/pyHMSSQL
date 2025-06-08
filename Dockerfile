FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY server/ ./server/
COPY shared/ ./shared/
COPY client/ ./client/
COPY orchestrator/ ./orchestrator/
COPY k8s/ ./k8s/

# Create necessary directories
RUN mkdir -p data/catalog data/indexes data/statistics data/tables logs

# Set environment variables
ENV PYTHONPATH=/app
ENV HMSSQL_DATA_DIR=/app/data
ENV HMSSQL_LOG_DIR=/app/logs

# Expose ports
EXPOSE 9999 5000 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import socket; s=socket.socket(); s.settimeout(5); s.connect(('localhost', 9999)); s.close()" || exit 1

# Default command (can be overridden)
CMD ["python", "server/server.py"]
