from dask.distributed import Client, LocalCluster, get_worker
import socket 
import os
import logging
import threading
import dask.dataframe as dd
import shutil
from distributed import Worker, Nanny
import asyncio
from torch import nn
from torch.utils.data import DataLoader
import pandas as pd
import torch
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

class SimpleModel(nn.Module):
    ''' simple model includes an embedding and linear layer, the output just x itself * 100'''
    def __init__(self):
        super().__init__()
        logging.info("i am being init")
        self.embedding = nn.Embedding(30000, 100)
        self.linear = nn.Linear(100, 1)
        with torch.no_grad():
            for i in range(30000):
                self.embedding.weight[i].fill_(float(i))
            self.linear.weight.fill_(1.0)
            self.linear.bias.zero_()

    def forward(self, x):
        x = self.embedding(x)
        x = self.linear(x)
        return x

def gpu_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    '''
      simulate a somewhat short gpu process
    '''
    worker = get_worker()
    logging.info(f"inside gpu_pipeline {worker}")
    logging.info(f"checking type: {type(df)}")
    # force it to turn into an actual dataframe so i can do dataloader
    df = df.compute()
    if not hasattr(worker, 'model'):
        worker.model = SimpleModel().to("cuda:0")
        worker.model.eval()
    x = df['word_count'].tolist()
    worker_id = f"{socket.gethostname()} | PID: {os.getpid()} | THREADID: {threading.get_ident()}"
    logging.info(f"{worker_id}: start gpu computing")
    dataloader = DataLoader(x, batch_size=1024, shuffle=False, drop_last=False)
    outputs = []
    for x in dataloader:
        x = x.to("cuda:0")
        outputs += worker.model(x).cpu().reshape(-1).tolist()
    df['max'] = outputs
    logging.info(f"start gpu sleeping")
    time.sleep(5)
    logging.info(f"end gpu pipeline")
    return df[['NOTE_ID', 'NOTE_DATE', 'word_count', 'max']]

def cpu_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    '''
      simulate a somewhat long cpu process
    '''
    worker = get_worker()
    logging.info(f"inside cpu_pipline {worker}")
    worker_id = f"{socket.gethostname()} | PID: {os.getpid()} | THREADID: {threading.get_ident()}"
    logging.info(f"{worker_id}: start cpu computing")
    df['max'] = df['max'] + df['word_count']
    logging.info(f"start cpu sleeping")
    time.sleep(20)
    logging.info(f"end cpu sleeping")
    return df

async def start_gpu_worker(cluster):
    await Nanny(cluster.scheduler_address, nthreads=1, worker_class=Worker, resources={"GPU": 1})

def main():
    # Start cluster with 3 CPU-only workers
    cluster = LocalCluster(n_workers=3, threads_per_worker=1, resources={"GPU": 0, "CPU_ONLY": 1})
    client = Client(cluster)

    # Add a GPU-enabled worker manually
    asyncio.run(start_gpu_worker(cluster))

    logging.info("finish setting up the client:")
    for addr, info in client.scheduler_info()["workers"].items():
        logging.info(f"full information ==== {info}")
        logging.info(f'checking {addr}, {info["resources"]}')

    input_dir = "/gpfs/gpfs1/dive/testing_word_count/"
    output_dir = "/gpfs/gpfs1/dive/testing_word_count_forwarded"
    # force a rewrite
    if os.path.exists(output_dir):
        logging.info(f"deleting {output_dir}")
        shutil.rmtree(output_dir)

    # Step 1: read parquet file
    ddf = dd.read_parquet(input_dir, engine='pyarrow', ignore_metadata_file=True)

    # Step 2: convert to delays and submit to gpu worker
    partitions = ddf.to_delayed()
    gpu_futures = client.map(gpu_pipeline, partitions, resources={'GPU': 1})

    # Step 3: do some cpu computations
    cpu_futures = client.map(cpu_pipeline, gpu_futures, resources={'CPU_ONLY': 1})

    # Step 4: gather back from gpu_futures
    result_ddf = dd.from_delayed(cpu_futures)
    
    # Step 5: write to parquet
    result_ddf.to_parquet(
        output_dir,
        engine='pyarrow',
        write_index=False,
        partition_on=['NOTE_DATE'],
        compute=True,
        write_metadata_file=False
    )

    logging.info(f"checking result: {result_ddf.head()}")
    logging.info(f"checking result: {result_ddf.shape.result}")

    # for keeping the dash board open
    time.sleep(3600)

if (__name__ == '__main__'):
    main()