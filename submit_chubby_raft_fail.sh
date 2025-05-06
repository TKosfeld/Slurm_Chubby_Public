#!/bin/bash
#SBATCH --job-name=chubby-lock
#SBATCH --nodes=5
#SBATCH --ntasks=5
#SBATCH --ntasks-per-node=1
#SBATCH --output=logfile/chubby-lock-%j.out
#SBATCH --error=logfile/chubby-lock-%j.err
#SBATCH --time=1:00:00

BASE_HTTP_PORT=8000
BASE_RAFT_PORT=9000
RAFT_DIR_BASE="raft_data"
LOG_DIR="logfile"
REPLICA_LIST_FILE="replica_list.txt"
LEASE_DURATION=30

declare -a REPLICA_ADDRESSES

NODES=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
NODES_ARRAY=($NODES)

# Function to launch a Chubby node
launch_chubby_node() {
    local NODE="$1"
    local NODE_INDEX="$2"
    local JOIN_ADDR="$3"
    local SINGLE_NODE="$4"

    local HTTP_PORT=$((BASE_HTTP_PORT + NODE_INDEX))
    local RAFT_PORT=$((BASE_RAFT_PORT + NODE_INDEX))
    local HTTP_BIND="${NODE}:${HTTP_PORT}"
    local RAFT_BIND="${NODE}:${RAFT_PORT}"
    local NODE_ID="${HTTP_BIND}"
    local RAFT_DIR="${RAFT_DIR_BASE}/${SLURM_JOB_ID}-${NODE_INDEX}"

    local LOG_FILE_OUT="${LOG_DIR}/chubby-lock-${SLURM_JOB_ID}_${NODE_INDEX}.out"
    local LOG_FILE_ERR="${LOG_DIR}/chubby-lock-${SLURM_JOB_ID}_${NODE_INDEX}.err"

    echo "[INFO] Launching node ${NODE_INDEX} on ${NODE}: Listen=${HTTP_BIND}, RaftBind=${RAFT_BIND}, ID=${NODE_ID}, RaftDir=${RAFT_DIR}, Join=${JOIN_ADDR}, Single=${SINGLE_NODE}, LeaseDuration=${LEASE_DURATION}s"

    mkdir -p "${RAFT_DIR}"

    srun --ntasks=1 --nodelist="${NODE}" --output="${LOG_FILE_OUT}" --error="${LOG_FILE_ERR}" bash -c "
        while true; do
            ./chubby-raft \
                -listen='${HTTP_BIND}' \
                -raftbind='${RAFT_BIND}' \
                -raftdir='${RAFT_DIR}' \
                -id='${NODE_ID}' \
                -join='${JOIN_ADDR}' \
                -single='${SINGLE_NODE}' \
                -lease_duration='${LEASE_DURATION}'

            echo '[WARN] Replica ${NODE_INDEX} crashed. Restarting seconds...' >> ${LOG_FILE_OUT}
        done
    " &
    
    REPLICA_ADDRESSES+=("${HTTP_BIND}")
}

NODE="${NODES_ARRAY[0]}"
launch_chubby_node "$NODE" 0 "" true

sleep 15 # Give the leader some time to start electing self

for i in "${!NODES_ARRAY[@]}"; do
    if [[ $i -gt 0 ]]; then
        NODE="${NODES_ARRAY[$i]}"
        JOIN_ADDR="${NODES_ARRAY[0]}:${BASE_HTTP_PORT}"
        launch_chubby_node "$NODE" "$i" "$JOIN_ADDR" false
    fi
done

echo "${REPLICA_ADDRESSES[@]}" > "$REPLICA_LIST_FILE"
echo "[INFO] Replica list written to $REPLICA_LIST_FILE"

wait
