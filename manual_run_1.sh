#!/bin/bash

FLAG_FILE="run.flag"
COMMAND="go run client_log_fail.go -op write -duration 30s -rps 500 -populate=false -out lease_logs/15000.csv"      

echo "[INFO] PID $$ waiting for flag: $FLAG_FILE"
while [ ! -f "$FLAG_FILE" ]; do
    sleep 1
done

echo "[INFO] PID $$ detected flag, running command..."
$COMMAND

