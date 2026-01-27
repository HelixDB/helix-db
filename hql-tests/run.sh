#!/bin/bash

## Handle different argument patterns


if [ $# -eq 1 ]; then
    # Single file number
    cargo run --profile dev --bin test -- $1
elif [ $# -eq 3 ] && [ "$2" = "branch" ]; then
    # File number with branch: run.sh 26 branch fixinghql-error-file26
    cargo run --profile dev --bin test -- $1 --branch $3
elif [ $# -eq 2 ] && [ "$1" = "branch" ]; then
    # Branch only: run.sh branch fixinghql-error-file26
    cargo run --profile dev --bin test -- --branch $2
elif [ $# -eq 3 ] && [ "$1" = "batch" ]; then
    # Batch mode: run.sh batch 10 1
    cargo run --profile dev --bin test -- --batch $2 $3
elif [ $# -eq 5 ] && [ "$1" = "batch" ] && [ "$4" = "branch" ]; then
    # Batch with branch: run.sh batch 10 1 branch fixinghql-error-file26
    cargo run --profile dev --bin test -- --batch $2 $3 --branch $5
else
    # Default: process all files
    cargo run --profile dev --bin test
fi