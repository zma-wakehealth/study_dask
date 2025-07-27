#!/bin/bash

#SBATCH --time 00:10:00
#SBATCH --job-name study_dask
#SBATCH -n 1
#SBATCH -p defq
#SBATCH --cpus-per-task 6
#SBATCH --mem 16G

source /home/zhma/study_dask/.venv/bin/activate

python test1.py
