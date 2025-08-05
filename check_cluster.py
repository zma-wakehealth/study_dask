import logging
from dask.distributed import Client
import argparse
import time

parser = argparse.ArgumentParser()
parser.add_argument('--scheduler', type=str, required=True, help="dask scheduler address")
args = parser.parse_args()


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

client = Client(args.scheduler)

# it needs to wait a little bit to let all workers connected
for _ in range(5):
    logging.info(client)
    # if len(client.scheduler_info()['workers']) > 0:
    #     break
    logging.info("waiting for workers to connect")
    time.sleep(10)
    for addr, info in client.scheduler_info()["workers"].items():
        logging.info(f"full information ==== {info}")
        logging.info(f'checking {addr}, {info["resources"]}')

client.shutdown()