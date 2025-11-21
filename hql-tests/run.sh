#!/bin/bash

## Handle different argument patterns


if [ $# -eq 1 ]; then
    # Single file number
    cargo run --profile dev --bin test -- $1
elif [ $# -eq 3 ] && [ "$1" = "batch" ]; then
    # Batch mode: run.sh batch 10 1
    cargo run --profile dev --bin test -- --batch $2 $3

elif [ $# -eq 4 ] && [ "$1" = "batch" ] && [ "$4" = "rocks" ]; then
    # Batch mode with rocks backend: run.sh batch 10 1 rocks
    cargo run --profile dev --bin test -- --batch $2 $3 --backend rocks
elif [ $# -eq 4 ] && [ "$1" = "batch" ] && [ "$4" = "lmdb" ]; then
    # Batch mode with lmdb backend: run.sh batch 10 1 lmdb
    cargo run --profile dev --bin test -- --batch $2 $3 --backend lmdb
else
    # Default: process all files
    cargo run --profile dev --bin test
fi
