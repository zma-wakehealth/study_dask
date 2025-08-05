#!/bin/bash

SCHEDULER_ADDRESS="$1"
NPROC="$2"
dask-worker $SCHEDULER_ADDRESS --name cpu_worker_$(hostname) --nthreads 1 --nworkers $NPROC --nanny --death-timeout 60 --local-directory /scratch/zhma/tmprun --resources "CPU_ONLY=1" &
CUDA_VISIBLE_DEVICE=0 dask-worker $SCHEDULER_ADDRESS --name gpu_worker_$(hostname) --nthreads 1 --nworkers 1 --nanny --death-timeout 60 --local-directory /scratch/zhma/tmprun --resources "GPU=1" &

wait