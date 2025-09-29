#!/bin/bash
# Load .env file
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Run migrate with any arguments passed to this script
migrate -database "$DATABASE_URL" -path migrations "$@"
