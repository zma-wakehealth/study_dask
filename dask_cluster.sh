#!/bin/bash
#SBATCH --job-name=dask-multi
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=2
#SBATCH --gpus-per-task=1
#SBATCH --mem=48G
#SBATCH --time=00:15:00
#SBATCH --output=dask-%j.out
#SBATCH --error=dask-%j.err
#SBATCH --nodelist=demon089,demon134

source .venv/bin/activate
chmod 744 ./start_worker.sh

SCHEDULER_HOST=$(scontrol show hostname $SLURM_JOB_NODELIST | head -n1)
SCHEDULER_PORT=8786
SCHEDULER_ADDRESS="tcp://${SCHEDULER_HOST}:${SCHEDULER_PORT}"

# Start the scheduler on the first node
if [[ $(hostname) == "$SCHEDULER_HOST" ]]; then
    echo "Starting scheduler on $(hostname)"
    dask-scheduler --port $SCHEDULER_PORT &
fi
sleep 10

# Start the workers on each node
echo "Starting workers on all nodes"
srun --ntasks=$SLURM_JOB_NUM_NODES --ntasks-per-node=1 ./start_worker.sh $SCHEDULER_ADDRESS 2 &
echo "get to sleeping"
sleep 30

# Run the actual workload
echo "cluster done set up"
#python check_cluster.py --scheduler $SCHEDULER_ADDRESS
python test5.py --scheduler $SCHEDULER_ADDRESS

wait
echo "done everything"
