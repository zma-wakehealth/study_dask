import dask.dataframe as dd
import os
import logging
import shutil
import socket
import threading
from dask.distributed import Client, LocalCluster
import time
from pandas.api.types import CategoricalDtype

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def main():
    cluster = LocalCluster(processes=True, n_workers=4, threads_per_worker=1)
    client = Client(cluster)
    logging.info(client)

    input_dir = "/gpfs/gpfs1/dive/testing/"
    output_dir = "/gpfs/gpfs1/dive/testing_copy"

    # force a rewrite
    if os.path.exists(output_dir):
        logging.info(f"deleting {output_dir}")
        shutil.rmtree(output_dir)

    ddf = dd.read_parquet(input_dir, engine='pyarrow', ignore_metadata_file=True)
    logging.info(f"ddf partitions:  {ddf.npartitions}")
    logging.info(f"ddf divisions: {ddf.divisions}")
    logging.info(ddf.head())

    ddf.to_parquet(
        output_dir,
        engine='pyarrow',
        write_index=False,
        partition_on=['NOTE_DATE'],
        compute=True,
        write_metadata_file=False
    )

    # read in back in to take a look
    ddf = dd.read_parquet(output_dir, engine='pyarrow')
    logging.info(ddf.head())

if (__name__ == '__main__'):
    main()