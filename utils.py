from dask.distributed import get_worker
import socket
import os

def check_data_transfer(info, worker_names):
    output_string = ''
    for worker, transfers in info.items():
        worker_name = worker_names[worker]
        output_string = output_string + worker + worker_name + '\n'
        for transfer in transfers:
            task = list(transfer['keys'].keys())
            total = transfer['total'] / 1024.0 / 1024.0
            who = transfer['who']
            line = f"  who={who}, task={task}, total={total}MB\n"
            output_string += line
    return output_string

