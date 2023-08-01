#!/bin/bash

set -e

# These values can be overridden by args
PORT=27018
DBPATH="./target/testdb"

# A POSIX variable
OPTIND=1 # Reset in case getopts has been used previously in the shell.

# Parse named parameters
while [[ $# -gt 0 ]]
do
  key="$1"

  case $key in
    -p|--port)
    PORT="$2"
    shift # past argument
    shift # past value
    ;;
    -d|--dbpath)
    DBPATH="$2"
    shift # past argument
    shift # past value
    ;;
  esac
done

# Clean up
function cleanup {
  echo "Shutting down MongoDB"
  kill -s SIGINT $MONGO_PID

  # Wait for mongod to exit
  while kill -0 "$MONGO_PID" >/dev/null 2>&1; do
    sleep 0.5
  done

  echo "Removing $DBPATH directory"
  rm -rf "$DBPATH"
}

# Trap the SIGINT signal (Ctrl+C) and SIGTERM signal (kill), and call cleanup
trap cleanup SIGINT SIGTERM

ulimit -n 100000

mkdir -p "$DBPATH"

# Start mongod in the background
mongod --quiet --dbpath "$DBPATH" --port "$PORT" --replSet "rs0" &
MONGO_PID=$!

# Retry up to 10 times with delay, waiting for MongoDB to start up
n=0
until [ "$n" -ge 10 ]
do
  echo "Checking if MongoDB is up..."
  if echo 'db.runCommand({ ping: 1 })' | mongosh --port "$PORT" --quiet; then
    break
  fi
  n=$((n+1))
  sleep 0.5
done

# Check if MongoDB is initialized
if ! echo 'rs.status().ok' | mongosh --port "$PORT" --quiet | grep -q 1; then
  echo 'rs.initiate()' | mongosh --port "$PORT"
  echo "Replica set initiated..."
fi

echo "MongoDB is ready"

# Wait for the MongoDB process to be stopped (by Ctrl+C for example)
wait $MONGO_PID
