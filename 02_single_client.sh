#!/bin/bash
#SBATCH --job-name=chubby-client
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=1GB
#SBATCH --nodes=1
#SBATCH --time=5:00
#SBATCH --output=multi_logs/client_%j.out
#SBATCH --error=multi_logs/client_%j.err
#SBATCH --exclusive

RPS=100
DURATION=60s
OUTFILE="multi_logs/client_${SLURM_JOB_ID}.csv"
MASTER="multi_logs/master_latencies.csv"

echo "[INFO] Starting client at $(date) on node $SLURMD_NODENAME"
echo "[INFO] Writing to $OUTFILE"

./client_log -op mixed -duration "$DURATION" -rps "$RPS" -populate=false -out "$OUTFILE"

(
    flock 200
    if [[ ! -f "$MASTER" ]]; then
        head -n 1 "$OUTFILE" > "$MASTER"
    fi
    tail -n +2 "$OUTFILE" >> "$MASTER"
) 200>"multi_logs/master_lock.lock"

echo "[INFO] Finished client at $(date)"

