from dask.distributed import Client, get_worker
import socket 
import os
import logging
import threading
import dask.dataframe as dd
import shutil
from torch import nn
from torch.utils.data import DataLoader
import pandas as pd
import torch
import time
import argparse
from collections import defaultdict
from utils import check_data_transfer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def setup_logging():
    logger = logging.getLogger()
    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)


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

def write_pipeline(df: pd.DataFrame, output_dir: str) -> pd.DataFrame:
    '''
      the write pipeline
    '''
    worker = get_worker()
    logging.info(f"inside cpu_pipline {worker}")
    worker_id = f"{socket.gethostname()} | PID: {os.getpid()} | THREADID: {threading.get_ident()}"
    logging.info(f"{worker_id}: start writing")
    df.to_parquet(output_dir, partition_cols=['NOTE_DATE'], index=False)

def main(scheduler):
    # Start cluster with 3 CPU-only workers
    client = Client(scheduler)

    # it needs to wait a little bit to let all workers connected
    for _ in range(3):
        logging.info(client)
        logging.info("waiting for workers to connect")
        time.sleep(10)
        for addr, info in client.scheduler_info()["workers"].items():
            logging.info(f"full information ==== {info}")
            logging.info(f'checking {addr}, {info["resources"]}')

    # set up a mapping bewteen node and workers
    node_to_workers = defaultdict(list)
    worker_names = dict()
    for addr, info in client.scheduler_info()["workers"].items():
        node_to_workers[info['host']].append(addr)
        worker_names[addr] = info['name']
    all_nodes = list(node_to_workers.keys())
    num_nodes = len(all_nodes)
    logging.info(f"checking node to worker map: {node_to_workers} {worker_names}")

    # force all the worker to set up logging
    client.run(setup_logging)
    logging.info(f"everyone sets up logging")

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

    # Step 3: need to do a loop to assign workers manually to avoid cross node communiations
    all_futures = []
    for i, part in enumerate(partitions):
        # select a node
        workers = all_nodes[i % num_nodes]
        # submit gpu and cpu job to this node only
        gpu_future = client.submit(gpu_pipeline, part, workers=workers, resources={'GPU': 1})
        cpu_future = client.submit(cpu_pipeline, gpu_future, workers=workers, resources={'CPU_ONLY': 1})
        # it may be actually better to allow some shuffling back to a ddf before it writes; it gives a better categorical NOTE_DATE columns
        # write_future = client.submit(write_pipeline, cpu_future, output_dir=output_dir, workers=workers, resources={"CPU_ONLY": 1})
        # all_futures.append(write_future)
        all_futures.append(cpu_future)

    # Step 4: gather back from gpu_futures
    result_ddf = dd.from_delayed(all_futures)
    
    # Step 5: write to parquet
    result_ddf.to_parquet(
        output_dir,
        engine='pyarrow',
        write_index=False,
        partition_on=['NOTE_DATE'],
        compute=True,
        write_metadata_file=False
    )

    time.sleep(5)
    logging.info(f"checking result: {result_ddf.head()}")
    logging.info(f"checking result: {result_ddf.shape}")

    logging.info(f"=========== checking outgoing transfer ============ ")
    info = client.run(lambda dask_worker: dask_worker.transfer_outgoing_log)
    logging.info(f"{check_data_transfer(info, worker_names)}")
    logging.info(f"=========== checking incoming transfer ============ ")
    info = client.run(lambda dask_worker: dask_worker.transfer_incoming_log)
    logging.info(f"{check_data_transfer(info, worker_names)}")

    time.sleep(5)
    logging.info(f"forcing a shuffle")
    tmp = result_ddf.groupby("word_count")['max'].sum().compute()
    logging.info(f"checking after groupby: {tmp}")

    logging.info(f"=========== checking outgoing transfer ============ ")
    info = client.run(lambda dask_worker: dask_worker.transfer_outgoing_log)
    logging.info(f"{check_data_transfer(info, worker_names)}")
    logging.info(f"=========== checking incoming transfer ============ ")
    info = client.run(lambda dask_worker: dask_worker.transfer_incoming_log)
    logging.info(f"{check_data_transfer(info, worker_names)}")

    # for keeping the dash board open
    time.sleep(600)

    client.shutdown()

if (__name__ == '__main__'):
    parser = argparse.ArgumentParser()
    parser.add_argument('--scheduler', type=str, required=True, help="dask scheduler address")
    args = parser.parse_args()
    main(args.scheduler)