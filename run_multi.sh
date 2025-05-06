#!/bin/bash

MAX_TASKS=20

for ((ntask=1; ntask<=MAX_TASKS; ntask++)); do
    echo "==> Launching $ntask client job(s)..."
    JOB_IDS=()

    for ((i=1; i<=ntask; i++)); do
        JOBID=$(sbatch 02_single_client.sh | awk '{print $NF}')
        echo "    -> Submitted client $i as job $JOBID"
        JOB_IDS+=("$JOBID")
    done

    # Wait for all jobs from this batch
    for JOBID in "${JOB_IDS[@]}"; do
        while squeue -j "$JOBID" > /dev/null 2>&1 && squeue -j "$JOBID" | grep -q "$JOBID"; do
            echo "       ... Waiting for job $JOBID to finish"
            sleep 100
        done
        echo "       -> Job $JOBID completed"
    done

    echo "==> All $ntask jobs completed for this round. Proceeding to next."
done

echo "All rounds submitted and completed (1 through $MAX_TASKS clients per round)."

