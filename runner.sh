#!/bin/bash

# Get the current branch from Travis CI environment variables.
BRANCH=$(if [ "$TRAVIS_PULL_REQUEST" == "false" ];
    then echo "$TRAVIS_BRANCH"; else echo "$TRAVIS_PULL_REQUEST_BRANCH"; fi)
export BRANCH
echo "Current Branch: $BRANCH"

# Start Docker Compose.
docker-compose build && docker-compose up

# Record the Test Runner Exit Code to return.
RUNNER_EXIT_CODE=$(docker wait kqm_testrunner_1)
echo "Received Test Runner Exit Code: $RUNNER_EXIT_CODE"

# Perform cleanup.
echo "Cleaning up docker-compose debris."
docker-compose kill && docker-compose rm -f

# Return the Test Exit Code.
echo "Done. Exiting with return code: $RUNNER_EXIT_CODE"
exit "$RUNNER_EXIT_CODE"
