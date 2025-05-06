#!/bin/bash

REPLICA_LIST_FILE="replica_list.txt"
KILL_SIGNAL="-9"  # -9 for force kill

read -a REPLICAS < "$REPLICA_LIST_FILE"

# Choose a random replica index
IDX=$(( RANDOM % ${#REPLICAS[@]} ))
TARGET="${REPLICAS[$IDX]}"
HOST=$(echo "$TARGET" | cut -d: -f1)
PORT=$(echo "$TARGET" | cut -d: -f2)

echo "[INFO] Targeting replica $TARGET on host $HOST port $PORT"

ssh "$HOST" "
    PID=\$(lsof -ti tcp:${PORT})
    if [ -n \"\$PID\" ]; then
        echo \"[INFO] Killing PID \$PID on $HOST (port $PORT)\"
        kill ${KILL_SIGNAL} \$PID
    else
        echo \"[WARN] No process found on port $PORT at $HOST\"
    fi
"

echo "[INFO] Kill complete. Exiting."
