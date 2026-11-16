#!/bin/bash
# Compile and run the plain-Java test harness. Exits non-zero on any failure.
set -e
cd "$(dirname "$0")"
rm -rf out-test
mkdir -p out-test
javac -encoding UTF-8 -d out-test $(find src/main/java src/test/java -name "*.java")
echo "Compiled OK."
java -cp out-test com.whitmandutko.cache.CacheTest
