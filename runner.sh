#!/bin/bash

# Start Docker Compose.
docker-compose build && docker-compose up

# Record the Test Runner Exit Code to return.
RUNNER_EXIT_CODE=$(docker wait kqm_test_runner_1)

# Perform cleanup.
docker-compose kill && docker-compose rm -f

# Return the Test Exit Code.
exit "$RUNNER_EXIT_CODE"
