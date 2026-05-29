#!/bin/bash
set -e
echo "Waiting for Cassandra to accept connections..."
until cqlsh cassandra -e "SELECT release_version FROM system.local" > /dev/null 2>&1; do
  echo "  cqlsh not ready, retrying in 5s..."
  sleep 5
done
echo "Cassandra ready. Applying schema..."
cqlsh cassandra -f /schema.cql
echo "Schema applied successfully."
