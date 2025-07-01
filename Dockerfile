FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for building and running
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    build-essential \
    python3-dev \
    cython3 \
    curl \
    git \
    openjdk-17-jdk \
    maven \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY server/ ./server/
COPY shared/ ./shared/
COPY client/ ./client/
COPY orchestrator/ ./orchestrator/
COPY k8s/ ./k8s/

# Build optimized B+ tree implementation
WORKDIR /app/server
RUN echo "Building optimized B+ tree..." && \
    if [ -f setup.py ]; then \
        echo "Found setup.py, building with setup.py..."; \
        python setup.py build_ext --inplace; \
    fi && \
    if [ -f build_bptree.sh ]; then \
        echo "Found build_bptree.sh, executing..."; \
        chmod +x build_bptree.sh && ./build_bptree.sh; \
    fi && \
    if [ -f requirements_bptree.txt ]; then \
        echo "Installing B+ tree specific requirements..."; \
        pip install --no-cache-dir -r requirements_bptree.txt; \
    fi && \
    echo "Verifying B+ tree compilation..." && \
    python -c "import sys; \
try: \
    from bptree_optimized import BPTreeOptimized; \
    print('Optimized B+ tree successfully compiled and importable'); \
except ImportError as e: \
    print(f'Failed to import optimized B+ tree: {e}'); \
    sys.exit(1)" && \
    echo "B+ tree build completed successfully"

# Build Java client with modern dependencies
WORKDIR /app/client/java_client
RUN if [ -f pom.xml ]; then \
        echo "Building Java client with Maven..."; \
        mvn clean compile package -DskipTests -q && \
        echo "Java client build completed"; \
        ls -la target/ || echo "No target directory found"; \
    else \
        echo "No Java client pom.xml found, skipping Java build"; \
    fi

# Return to app directory
WORKDIR /app

# Create necessary directories
RUN mkdir -p data/catalog data/indexes data/statistics data/tables logs

# Set environment variables
ENV PYTHONPATH=/app
ENV HMSSQL_DATA_DIR=/app/data
ENV HMSSQL_LOG_DIR=/app/logs
ENV CFLAGS="-O3"

# Expose ports
EXPOSE 9999 5000 8080 8081

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import socket; s=socket.socket(); s.settimeout(5); s.connect(('localhost', 9999)); s.close()" || exit 1

# Default command (can be overridden)
CMD ["python", "server/server.py"]
