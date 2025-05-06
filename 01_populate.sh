#!/bin/bash
#SBATCH --job-name=populate-locks
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --time=00:01:00
#SBATCH --output=multi_logs/populate.out
#SBATCH --error=multi_logs/populate.err

echo "[$(date)] Running prepopulation task..."
./client_log -op write -duration 1s -rps 1 -populate=true -out /dev/null

