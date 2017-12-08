#!/bin/bash

# Get the current branch from Travis CI environment variables.
BRANCH=$(if [ "$TRAVIS_PULL_REQUEST" == "false" ];
    then echo $TRAVIS_BRANCH; else echo $TRAVIS_PULL_REQUEST_BRANCH; fi)
echo "Current Branch: $BRANCH"

# Start Docker Compose.
docker-compose build && BRANCH=$BRANCH docker-compose up

# Record the Test Runner Exit Code to return.
RUNNER_EXIT_CODE=$(docker wait kqm_test_runner_1)

# Perform cleanup.
docker-compose kill && docker-compose rm -f

# Return the Test Exit Code.
exit "$RUNNER_EXIT_CODE"
