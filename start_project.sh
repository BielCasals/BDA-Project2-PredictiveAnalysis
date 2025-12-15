#!/bin/bash

# ====================================================
#  Automated script for starting project
# ====================================================

# 1. Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}--- [1/4] Starting MongoDB Service ---${NC}"
# Check if running, if not start it
if pgrep -x "mongod" > /dev/null
then
    echo -e "${GREEN}MongoDB is already running.${NC}"
else
    # Try systemd first, fall back to service
    sudo service mongod start || sudo systemctl start mongod
    echo -e "${GREEN}MongoDB started.${NC}"
fi

echo -e "${BLUE}--- [2/4] Activating Virtual Environment ---${NC}"
# Adjust the path if your venv is named differently
source de_venv/bin/activate

echo -e "${BLUE}--- [3/4] Starting MLflow Tracking Server ---${NC}"
# Run in background (&)
mlflow ui --port 5000 &
echo -e "${GREEN}MLflow running at http://localhost:5000 (PID: $!)${NC}"


echo -e "${BLUE}--- [4/4] Starting Airflow Standalone ---${NC}"
# Airflow standalone runs scheduler, webserver, and triggerer
# We run this in the foreground so you can see the logs
# OR run in background if you prefer
echo -e "${GREEN}Starting airflow"
echo -e "${GREEN}Credentials stored in 'standalone_admin_password.txt'${NC}"

# Open Browser (Works in WSL to open Windows Chrome/Edge)
# explorer.exe "http://localhost:8080"
# explorer.exe "http://localhost:5000"

# Start Airflow
airflow db init

airflow users create \
    --username admin \
    --firstname User \
    --lastname Student \
    --role Admin \
    --email admin@example.com \
    --password admin

echo -e "${GREEN} Starting scheduler${NC}"

airflow scheduler&

echo -e "${GREEN} Starting web server${NC}"

airflow webserver --port 8080





