#!/bin/bash

# HMSSQL Orchestrator Startup Script
# This script starts the HMSSQL Database Orchestrator with proper configuration

echo "🎯 Starting HMSSQL Database Orchestrator..."

# Check if we're in a virtual environment
if [[ "$VIRTUAL_ENV" == "" ]]; then
    echo "⚠️  Warning: No virtual environment detected. Consider using a virtual environment."
fi

# Check if required packages are installed
python3 -c "import flask, psutil, requests" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "❌ Required dependencies missing. Installing..."
    pip install flask psutil requests
fi

# Set default values
HOST=${HOST:-"0.0.0.0"}
PORT=${PORT:-5001}
DEBUG=${DEBUG:-false}
LOG_LEVEL=${LOG_LEVEL:-"INFO"}

# Change to the orchestrator directory
cd "$(dirname "$0")"

echo "📡 Web interface will be available at: http://${HOST}:${PORT}"
echo "🔍 Log level: ${LOG_LEVEL}"

# Start the orchestrator
if [ "$DEBUG" = "true" ]; then
    python3 orchestrator.py --host "$HOST" --port "$PORT" --debug --log-level "$LOG_LEVEL"
else
    python3 orchestrator.py --host "$HOST" --port "$PORT" --log-level "$LOG_LEVEL"
fi
