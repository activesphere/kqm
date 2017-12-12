#!/bin/bash
trap 'echo "SIGINT received. Shutting down."; kill $$' INT

while true
do
    echo "$1 running, waiting for SIGINT."
    sleep 10
done
