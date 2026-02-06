#!/bin/bash
set -e

# Wait for DB using Python
echo "Waiting for database..."
while ! python -c "import socket; s = socket.socket(); s.settimeout(1); s.connect(('${DATABASE_HOST}', ${DATABASE_PORT})); s.close()" 2>/dev/null; do
  sleep 1
done
echo "Database is ready!"

# Wait for Redis using Python
echo "Waiting for Redis..."
while ! python -c "import socket; s = socket.socket(); s.settimeout(1); s.connect(('${REDIS_HOST}', ${REDIS_PORT})); s.close()" 2>/dev/null; do
  sleep 1
done
echo "Redis is ready!"

case "$1" in
  app-gunicorn)
    echo "Starting Superset..."
    /usr/bin/run-server.sh
    ;;
  worker)
    echo "Starting Celery worker..."
    celery --app=superset.tasks.celery_app:app worker --pool=prefork -O fair -c 4
    ;;
  beat)
    echo "Starting Celery beat..."
    celery --app=superset.tasks.celery_app:app beat --pidfile /tmp/celerybeat.pid --schedule /tmp/celerybeat-schedule
    ;;
  *)
    echo "Unknown command: $1"
    exit 1
    ;;
esac
