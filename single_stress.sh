#!/bin/bash
#SBATCH --job-name=ramp-stress
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --time=2:00:00
#SBATCH --output=logs/ramp_stress_%j.out
#SBATCH --error=logs/ramp_stress_%j.err

X=${1:-1000}   
MULT=${2:-2}              
DURATION=60               
CMD="./client_log"        

RPS=10
while [[ "$RPS" -le "$X" ]]; do
  echo "Running RPS=$RPS for $DURATION seconds"
  $CMD -op mixed -duration ${DURATION}s -populate=true -rps $RPS -out logs/mixed_rps${RPS}.csv
  RPS=$(( RPS * MULT ))
done

RPS=10
while [[ "$RPS" -le "$X" ]]; do
  echo "Running RPS=$RPS for $DURATION seconds"
  $CMD -op write -populate=true -duration ${DURATION}s -rps $RPS -out logs/write_rps${RPS}.csv
  RPS=$(( RPS * MULT ))
done

RPS=10
while [[ "$RPS" -le "$X" ]]; do
  echo "Running RPS=$RPS for $DURATION seconds"
  $CMD -op read -duration ${DURATION}s -populate=true -rps $RPS -out logs/read_rps${RPS}.csv
  RPS=$(( RPS * MULT ))
done
