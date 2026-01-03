#!/bin/bash

# Number of requests to make
TOTAL_REQUESTS=100000
# Number of concurrent requests (adjust based on your system)
PARALLEL_JOBS=200

echo "Starting $TOTAL_REQUESTS requests with $PARALLEL_JOBS concurrent connections..."

# Generate sequence and run in parallel
seq 1 $TOTAL_REQUESTS | xargs -P $PARALLEL_JOBS -I {} bash -c '
  curl -s -X POST http://localhost:6969/get -H "Content-Type: application/json" -d "{\"id\": \"1f0bbb68-0010-6305-b7df-010203040506\"}" > /dev/null
  if [ $(({} % 1000)) -eq 0 ]; then
    echo "Completed {} requests"
  fi
'

echo "All $TOTAL_REQUESTS requests completed!"