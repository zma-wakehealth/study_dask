import dask.dataframe as dd
import os
import logging
import shutil
import socket
import threading
from dask.distributed import Client, LocalCluster
import time
from pandas.api.types import CategoricalDtype

log_filename = 'convert_note_date_to_str.log'
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    filename=log_filename
)

def compute_word_count(df):
    worker_id = f"{socket.gethostname()} | PID: {os.getpid()} | THREADID: {threading.get_ident()}"

    # Option 3: recat here??
    # tmp = df['NOTE_DATE'].value_counts().to_dict()
    # categories = [k for k, v in tmp.items() if v > 0]
    # cat_type = CategoricalDtype(categories=categories)
    # df['NOTE_DATE'] = df['NOTE_DATE'].astype(cat_type)
    logging.info(f"{worker_id} Processing partition with {len(df)} rows with {df['NOTE_DATE'].value_counts()}")

    # actual calculation
    df['word_count'] = df['NOTE_TEXT'].str.split().str.len()
    # time.sleep(60)
    return df[['NOTE_ID', 'word_count', 'NOTE_DATE']]

def main():
    cluster = LocalCluster(processes=True, n_workers=4, threads_per_worker=1)
    client = Client(cluster)
    logging.info(client)

    input_dir = "/gpfs/gpfs1/dive/testing/"
    output_dir = "/gpfs/gpfs1/dive/testing_word_count"

    # force a rewrite
    if os.path.exists(output_dir):
        logging.info(f"deleting {output_dir}")
        shutil.rmtree(output_dir)

    ddf = dd.read_parquet(input_dir, engine='pyarrow', ignore_metadata_file=True)
    logging.info(f"ddf partitions:  {ddf.npartitions}")
    logging.info(f"ddf divisions: {ddf.divisions}")
    logging.info(ddf.head())

    # Option 2: this will work but seems wasting a lot of memory
    ddf['NOTE_DATE'] = ddf['NOTE_DATE'].astype(str)

    ddf_result = ddf.map_partitions(compute_word_count, meta={'NOTE_ID':str, 'word_count':int, 'NOTE_DATE':str})
    logging.info(f"result partitions:  {ddf_result.npartitions}")
    logging.info(f"result divisions: {ddf_result.divisions}")

    # this errors
    # ddf_result = ddf_result.set_index("NOTE_DATE", shuffle="tasks").reset_index()

    # Option 1: this is suggested in the document to avoid many files
    # ddf_result = ddf_result.shuffle(on=['NOTE_DATE'])

    # save to graph to take a look
    ddf_result.visualize()

    ddf_result.to_parquet(
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