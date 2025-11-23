#!/bin/bash
# Start application for different campuses

CAMPUS=${1:-a}
PORT=${2:-8000}

if [ "$CAMPUS" != "a" ] && [ "$CAMPUS" != "b" ] && [ "$CAMPUS" != "c" ]; then
    echo "Usage: $0 [a|b|c] [port]"
    echo "Example: $0 a 8000    # Start campus A on port 8000"
    echo "         $0 b 8001    # Start campus B on port 8001"
    echo "         $0 c 8002    # Start campus C on port 8002"
    exit 1
fi

echo "=========================================="
echo "Starting Campus ${CAMPUS^^} on port $PORT"
echo "=========================================="

# Copy appropriate env file
cp .env.$CAMPUS .env
echo "Using .env.$CAMPUS configuration"

# Activate conda environment and start app
conda activate ddbs_course
uvicorn app.main:app --reload --host 0.0.0.0 --port $PORT

