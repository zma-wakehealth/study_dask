from dask_jobqueue import SLURMCluster
from dask.distributed import Client, get_worker
import logging
import time
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    filename='./log/log.txt'
)

def main():

    script_path = os.path.abspath("start_worker.sh")

    cluster = SLURMCluster(
        queue="defq",
        cores=4,
        memory="16GB",
        processes=4,
        local_directory='/scratch/zhma/tmprun',
        log_directory='./log',
        walltime="00:15:00",
        job_script_prologue = [
            "source /home/zhma/study_dask/.venv/bin/activate"
        ],
        job_extra_directives =[
            #"--cpus-per-task 6",  # this is generated from processes=4
            "--nodelist demon107",
            #"--job-name study_dask"
        ],
        shebang = "#!/bin/bash",
        # worker_command=[script_path]
    )
    cluster.scale(jobs=1)
    logging.info(cluster.job_script())
    client = Client(cluster)

    # it needs to wait a little bit to let all workers connected
    for _ in range(10):
        logging.info(client)
        # if len(client.scheduler_info()['workers']) > 0:
        #     break
        logging.info("waiting for workers to connect")
        time.sleep(10)
        for addr, info in client.scheduler_info()["workers"].items():
            logging.info(f"full information ==== {info}")
            logging.info(f'checking {addr}, {info["resources"]}')
    
    time.sleep(120)

if (__name__ == '__main__'):
    main()