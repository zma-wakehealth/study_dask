from dask.distributed import Client, LocalCluster, get_worker
import socket 
import os
import logging
import threading
import dask.dataframe as dd
import shutil
import time
from dask_cuda import CUDAWorker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def compute_word_count(df):
    worker_id = f"{socket.gethostname()} | PID: {os.getpid()} | THREADID: {threading.get_ident()}"
    # logging.info(f"{worker_id} Processing partition with {len(df)} rows with {df['NOTE_DATE'].value_counts()}")
    logging.info(f"{worker_id} Processing partition with {len(df)} rows")

    # actual calculation
    df['word_count'] = df['NOTE_TEXT'].str.split().str.len()
    return df[['NOTE_ID', 'word_count', 'NOTE_DATE']]

def main():
    # cluster = LocalCluster(
    #     n_workers=4,
    #     processes=True,
    #     threads_per_worker=1,
    #     resources={"GPU":0}
    # )
    # client = Client(cluster)

    # while len(client.scheduler_info()["workers"]) < 4:
    #     time.sleep(0.5)

    # def assign_gpu(gpu_worker=None):
    #     print("get into here")
    #     worker = get_worker()
    #     print("inside worker=", gpu_worker)
    #     gpu_worker.resources["GPU"] = 1
    # all_workers = list(client.scheduler_info()["workers"].keys())
    # gpu_worker = all_workers[-1]
    # # print(all_workers)
    # client.run(assign_gpu, workers=[all_workers[-1]], gpu_worker=gpu_worker)

    # Start cluster with 3 CPU-only workers
    cluster = LocalCluster(n_workers=3, threads_per_worker=1, resources={"GPU": 0})
    client = Client(cluster)

    # Add a GPU-enabled worker manually
    from distributed import Worker, Nanny
    import asyncio

    async def start_gpu_worker():
        print("check 1")
        # this CUDAWorker doesn't work
        # await Nanny(scheduler_ip=cluster.scheduler_address,
        #             nthreads=1, 
        #             worker_class=CUDAWorker,
        #             scheduler=cluster.scheduler_address)
        # await cluster._start_worker(
        #     name='gpu-worker',
        #     cls=Worker,
        #     options={'nthreads':1, "resources": {"GPU": 0}}
        # )
        await Nanny(cluster.scheduler_address, nthreads=1, worker_class=Worker, resources={"GPU": 1})

    # asyncio.get_event_loop().run_until_complete(start_gpu_worker())
    asyncio.run(start_gpu_worker())

    # try dask-cuda
    # cluster = LocalCUDACluster(CUDA_VISIBLE_DEVICES="0", n_workers=4, threads_per_worker=1)

    logging.info("finish setting up the client:")
    for addr, info in client.scheduler_info()["workers"].items():
        logging.info(f"full information ==== {info}")
        logging.info(f'checking {addr}, {info["resources"]}')

    input_dir = "/gpfs/gpfs1/dive/testing/"
    output_dir = "/gpfs/gpfs1/dive/testing_word_count"

    # force a rewrite
    if os.path.exists(output_dir):
        logging.info(f"deleting {output_dir}")
        shutil.rmtree(output_dir)

    ddf = dd.read_parquet(input_dir, engine='pyarrow', ignore_metadata_file=True)
    ddf = ddf.map_partitions(compute_word_count, meta={'NOTE_ID':str, 'word_count':int, 'NOTE_DATE':str})
    
    ddf.to_parquet(
        output_dir,
        engine='pyarrow',
        write_index=False,
        partition_on=['NOTE_DATE'],
        compute=True,
        write_metadata_file=False
    )

if (__name__ == '__main__'):
    main()