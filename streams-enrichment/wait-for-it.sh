#!/bin/sh
# wait-for-it.sh
# waits for the Kafka and Postgres containers to be ready

set -e

# Wait for Kafka
echo "Waiting for Kafka to be ready..."
until nc -z kafka 29092; do
  echo "Kafka not ready, sleeping..."
  sleep 1
done
echo "Kafka is ready."

# Wait for PostgreSQL
echo "Waiting for PostgreSQL to be ready..."
until pg_isready -h postgresql -p 5432 -U user; do
  echo "PostgreSQL not ready, sleeping..."
  sleep 1
done
echo "PostgreSQL is ready."

# Now, start the application
exec ./start-app.sh