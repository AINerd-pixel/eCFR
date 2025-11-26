#!/bin/bash

# FastAPI Startup Script for eCFR Agencies API

echo "Starting eCFR Agencies API..."

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source .venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Run the FastAPI application
echo "Starting FastAPI server on http://0.0.0.0:8000"
echo "API documentation will be available at http://0.0.0.0:8000/docs"
python agency_api.py