# Understanding How Dask Works

## Overview

This repository contains experiments to explore how Dask works.

## Experiment: test1.py

This tests how dask handles reading and writing Parquet files in a Hive-style directory structure. It tests the following workflow:

1. Read Parquet files in a Hive-style structure  
2. Transform the data (e.g., compute word counts)  
3. Write the result back to Parquet in Hive-style format  

A known issue is that Dask may generate many output files that contain only schema metadata and no actual rows (see the official documentation [here](https://docs.dask.org/en/latest/dataframe-hive.html)). For our simple case where the input is already partitioned by date, I think we can fix it. The underlying issue is that the date is stored as a categorical column, and each partition can see all the values—even if there are no rows for the other dates in that partition.

### Performance Comparison

| Strategy                              | Single Output File | Processing Time |
|---------------------------------------|---------------------|------------------|
| No changes                            | No                  | 9 sec            |
| Convert date (categorical) to string  | Yes                 | 9 sec            |
| Recompute categorical inside function | Yes                 | 12 sec           |
| Shuffle                               | Yes                 | 50 sec           |

**Main observation:** It seems converting date columns to strings is a simple and effective fix unless memory is extremely constrained.

Details are in these log files:
- add_shuffle_before_to_parquet.log
- convert_note_date_to_str.log
- doing_nothing.log
- recat_inside_compute_word_count.log

This experiment was run using the following SLURM settings:

```bash
#SBATCH --time 00:10:00
#SBATCH --job-name study_dask
#SBATCH -n 1
#SBATCH -p defq
#SBATCH --cpus-per-task 6
#SBATCH --mem 16G
```

Note: Although 6 CPUs were requested, only 4 were used in test1.py to observe how Dask manages available resources.


## Local Cluster with 1 GPU: test3.py
This experiment sets up a local Dask cluster utilizing a single GPU. The trick is to attach a **Nanny** process after initializing the standard LocalCluster.

## Cross-Node Cluster: test6.py + dask_cluster.sh + start_worker.sh
This setup enables a Dask cluster across multiple nodes. Here's a breakdown of the related experiments:

- test4.py: An unsuccessful attempt to configure the cluster purely with Python. It is just too complicated, I gave up.
- test5.py: Nearly successful, but the default setting allowed cross-node communication.
- test6.py: Successfully limits communication to intra-node only (except for the final `.to_parquet` operation). This is achieved by explicitly specifying node configurations.
- dask_cluster.sh and start_worker.sh: Bash scripts used to simplify and reliably set up the cluster across nodes, proving to be much easier than the Python-only approach.
- test5.err and test6.err: detail comparison between test5.py and test6.py; scroll down to look for "checking incoming transfer" and "checking outgoing transfer"
