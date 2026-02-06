#!/bin/bash
set -e

# Wait for DB using Python
echo "Waiting for database..."
while ! python -c "import socket; s = socket.socket(); s.settimeout(1); s.connect(('${DATABASE_HOST}', ${DATABASE_PORT})); s.close()" 2>/dev/null; do
  sleep 1
done
echo "Database is ready!"

echo "Initializing Superset..."

# Initialize the database
superset db upgrade

# Create admin user
superset fab create-admin \
  --username ${ADMIN_USERNAME:-admin} \
  --firstname ${ADMIN_FIRSTNAME:-Admin} \
  --lastname ${ADMIN_LASTNAME:-User} \
  --email ${ADMIN_EMAIL:-admin@asan.org} \
  --password ${ADMIN_PASSWORD:-admin} || true

# Initialize roles and permissions
superset init

echo "Superset initialization complete!"
