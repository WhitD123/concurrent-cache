#!/bin/bash
# Build and run the ConcurrentCache server.
# Usage: ./run.sh [port] [capacity] [snapshotPath]
# Defaults: port 8080, capacity 1000, snapshot data/snapshot.txt
set -e
cd "$(dirname "$0")"
mkdir -p out data
javac -encoding UTF-8 -d out $(find src/main/java -name "*.java")
echo "Compiled OK."
java -cp out com.whitmandutko.cache.CacheServer "${1:-8080}" "${2:-1000}" "${3:-data/snapshot.txt}"
