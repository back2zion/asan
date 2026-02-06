#!/bin/bash
set -e

# Run original Docker entrypoint script in the background to start postgres
/usr/local/bin/docker-entrypoint.sh "$@" &

echo "Waiting for PostgreSQL to start..."
until pg_isready -U $POSTGRES_USER -d $POSTGRES_DB; do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 1
done
echo "PostgreSQL is up - executing command"

if [ -f "/tmp/omop.sql" ]; then
  echo "Importing OMOP CDM data from /tmp/omop.sql"
  psql -U $POSTGRES_USER -d $POSTGRES_DB -f /tmp/omop.sql
  echo "OMOP CDM data imported successfully."
else
  echo "WARNING: /tmp/omop.sql not found. OMOP CDM data not imported."
fi

# Wait indefinitely for the postgres process to finish if it was started in the background
wait %1