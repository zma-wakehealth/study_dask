#!/bin/bash

#SBATCH --time 00:10:00
#SBATCH --job-name study_dask
#SBATCH -n 1
#SBATCH -p defq
#SBATCH --cpus-per-task 1
#SBATCH --mem 16G
#SBATCH --nodelist demon107

source /home/zhma/study_dask/.venv/bin/activate

python test4.py
