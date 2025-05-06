#!/bin/bash

FLAG_FILE="run.flag"
COMMAND="./client_log -op mixed -duration 120s -rps 100 -populate=false -out manual_multi/mixed_multi_13.csv"      

echo "[INFO] PID $$ waiting for flag: $FLAG_FILE"
while [ ! -f "$FLAG_FILE" ]; do
    sleep 1
done

echo "[INFO] PID $$ detected flag, running command..."
$COMMAND

