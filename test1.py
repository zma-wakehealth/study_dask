import dask.dataframe as dd
import os
import logging
import shutil
import socket
import threading

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def compute_word_count(df):
    worker_id = f"{socket.gethostname()} | PID: {os.getpid()} | THREADID: {threading.get_ident()}"
    logging.info(f"{worker_id} Processing partition with {len(df)} rows")
    df['word_count'] = df['NOTE_TEXT'].str.split().str.len()
    return df[['NOTE_ID', 'word_count', 'NOTE_DATE']]

def main():
    input_dir = "/gpfs/gpfs1/dive/testing/"
    output_dir = "/gpfs/gpfs1/dive/testing_word_count"

    # force a rewrite
    if os.path.exists(output_dir):
        logging.info(f"deleting {output_dir}")
        shutil.rmtree(output_dir)

    ddf = dd.read_parquet(input_dir, engine='pyarrow')
    logging.info(ddf.head())

    ddf_result = ddf.map_partitions(compute_word_count, meta={'NOTE_ID':str, 'word_count':int, 'NOTE_DATE':str})
    # save to graph to take a look
    ddf_result.visualize()
    logging.info(f"result partitions:  {ddf_result.npartitions}")

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