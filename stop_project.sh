#!/bin/bash
echo "Stopping all project services..."

# Kill Airflow
pkill -f airflow
echo "Airflow stopped."

# Kill MLflow
pkill -f mlflow
echo "MLflow stopped."

pkill -f gunicorn

# Kill Mongo (Optional, usually okay to leave running)
sudo service mongod stop
echo "MongoDB stopped."

echo "All services stopped."

#!/bin/bash

# ====================================================
#  PROJECT 2.0 - CLEANUP SCRIPT
# ====================================================

echo "--- Stopping Project Services ---"

# 1. Kill Airflow (Port 8080)
# lsof -t -i :8080 returns the PIDs. xargs passes them to kill -9.
# The -r flag prevents errors if no process is found.
if lsof -t -i :8080 > /dev/null; then
    echo "Killing processes on port 8080..."
    lsof -t -i :8080 | xargs -r kill -9
else
    echo "Port 8080 is already free."
fi

# 2. Kill MLflow (Port 5000)
if lsof -t -i :5000 > /dev/null; then
    echo "Killing processes on port 5000..."
    lsof -t -i :5000 | xargs -r kill -9
else
    echo "Port 5000 is already free."
fi

# 3. Safety Net: Kill by name just in case
pkill -f "airflow"
pkill -f "gunicorn"

echo "--- All services stopped. Ports released. ---"