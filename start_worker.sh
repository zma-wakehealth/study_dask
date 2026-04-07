#!/bin/bash

SCHEDULER_ADDRESS="$1"
NPROC="$2"

echo "starting worker $(SCHEDULER_ADDRESS) $(NPROC)"

LOCAL_DIR="/scratch/$USER/dask-tmp-$SLURM_JOB_ID"
mkdir -p $LOCAL_DIR

# start CPU workers
dask-worker $SCHEDULER_ADDRESS --name cpu_worker_$(hostname)_%(rank)s --nthreads 1 --nworkers $NPROC --nanny --death-timeout 60 --local-directory $LOCAL_DIR --resources "CPU_ONLY=1" &

# start GPU workers per GPU
NUM_GPUS=$(nvidia-smi --query-gpu=name --format=csv,noheader | wc -l)
for ((i=0; i<$NUM_GPUS; i++)); do
  CUDA_VISIBLE_DEVICES=$i dask-worker $SCHEDULER_ADDRESS --name gpu_worker_$(hostname)_gpu$i --nthreads 1 --nworkers 1 --nanny --death-timeout 60 --local-directory $LOCAL_DIR --resources "GPU=1" &
done

wait