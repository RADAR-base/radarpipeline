# RADAR-Pipeline SLURM Integration for Big Data

This document provides an overview of how to use RADAR-Pipeline with SLURM for processing big data. It covers the necessary configurations, commands, and examples to effectively utilize RADAR-Pipeline in a SLURM environment.

## Overview

RADAR-Pipeline is designed to handle large datasets efficiently, and integrating it with SLURM allows for distributed processing across multiple nodes. This setup is ideal for big data applications where parallel processing can significantly reduce computation time.

## What is SLURM?

SLURM (Simple Linux Utility for Resource Management) is an open-source job scheduler used in many high-performance computing (HPC) environments. It manages resources and job scheduling, allowing users to run jobs on a cluster of computers.

## Setting Up RADAR-Pipeline with SLURM

To use RADAR-Pipeline with SLURM, you need to ensure that your environment is properly configured. Follow these steps:

**Install RADAR-Pipeline**: Ensure that RADAR-Pipeline is installed in your environment. You can install it using pip:

   ```bash
   pip install radarpipeline
   ```

**Configure SLURM**: Make sure your SLURM environment is set up correctly. This includes having access to a SLURM cluster and the necessary permissions to submit jobs.

```bash
   # Check SLURM status
   sinfo

   # Submit a test job
   sbatch --wrap="echo Hello, SLURM"
```

### How it Works

Since RADAR-Pipeline uses Apache Spark for distributed data processing, it can be integrated with SLURM to manage Spark jobs across a cluster. The integration allows you to submit Spark jobs as SLURM jobs, leveraging SLURM's resource management capabilities. 

### Example SLURM Job Script

1. Create a `run_radarpipeline.py` script to submit your RADAR-Pipeline job:

```python
from radarpipeline import radarpipeline
from radarpipeline.common import utils
import sys
import os

#os.chdir("/scratch/users/k2039563/radarpipeline")
config_path = "./config.yaml"
config = utils.read_yaml(config_path)
print(config)
master_url = sys.argv[1]
config['spark_config']['spark_master'] = master_url
print(config)
radarpipeline.run(config)
#os.chdir("/scratch/users/k2039563/spark-test/")
```

The code takes the Spark master URL as a command-line argument and updates the configuration accordingly.

2. Create a SLURM job script `submit_radarpipeline.sh`:

#### Setting up the environment

```bash
#!/bin/bash
#SBATCH --nodes=2
#SBATCH --mem-per-cpu=32G
#SBATCH --cpus-per-task=8
#SBATCH --ntasks-per-node=2
#SBATCH --output=sparkjob-%j.out

## --------------------------------------
## 0. Preparation
## --------------------------------------

# load the Spark module
module purge
#module load openjdk/1.8.0_265-b01-gcc-13.2.0
module load anaconda3/2022.10-gcc-13.2.0
#conda init
#conda activate base
eval "$(conda shell.bash hook)"
echo $(conda info --envs)
conda activate radarpipeline3.12
module load spark
#export SPARK_HOME=/users/k2039563/.conda/envs/radarpipeline3.12/lib/python3.12/site-packages/pyspark
#export SPARK_HOME=/software/spackages_v0_21_prod/apps/linux-ubuntu22.04-zen2/gcc-13.2.0/spark-3.5.1-b3xmtsg3lsy3unttwyuo4ajudh6cspdn/
#export PATH=$PATH:/users/k2039563/.conda/envs/radarpipeline3.12/bin/spark
#echo $JAVA_HOME
# identify the Spark cluster with the Slurm jobid
export SPARK_IDENT_STRING=$SLURM_JOBID

# prepare directories
export SPARK_WORKER_DIR=${SPARK_WORKER_DIR:-$HOME/.spark/worker}
export SPARK_LOG_DIR=${SPARK_LOG_DIR:-$HOME/.spark/logs}
export SPARK_LOCAL_DIRS=${SPARK_LOCAL_DIRS:-/tmp/spark}
mkdir -p $SPARK_LOG_DIR $SPARK_WORKER_DIR
```

The script sets up the environment, loads necessary modules, and prepares directories for Spark logs and worker data.

#### Starting the Spark Cluster

```bash
## --------------------------------------
## 1. Start the Spark cluster master
## --------------------------------------
if [ -z "$SSH_AUTH_SOCK" ] ; then
  eval `ssh-agent -s`
  ssh-add
fi

echo $(python --version)
echo $(python3.12 --version)
#https://stackoverflow.com/questions/33806450/how-to-find-sparks-installation-directory
#export SPARK_HOME="$(find_spark_home.py)"
#export SPARK_HOME="/software/spackages_v0_21_prod/apps/linux-ubuntu22.04-zen2/gcc-13.2.0/spark-3.5.1-b3xmtsg3lsy3unttwyuo4ajudh6cspdn"
echo $SPARK_HOME
export SPARK_MASTER_IP=$( hostname )
MASTER_NODE=$( scontrol show hostname $SLURM_NODELIST | head -n 1 )
echo $MASTER_NODE
echo $SPARK_MASTER_IP
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
spark-class org.apache.spark.deploy.master.Master --ip $SPARK_MASTER_IP --port "$SPARK_MASTER_PORT " --webui-port "$SPARK_MASTER_WEBUI_PORT"
#bash start-master.sh
sleep 60
echo "Master started"
#MASTER_URL=$(grep -Po '(?=spark://).*' \
#             $SPARK_LOG_DIR/spark-${SPARK_IDENT_STRING}-org.*master*.out)
MASTER_URL=$(grep -Po '(?=spark://).*' \
             ./sparkjob-${SPARK_IDENT_STRING}.out)
```
The script starts the Spark master node and retrieves the master URL from the log file. It also sets up the necessary environment variables for Spark.

#### Starting Spark Workers

```bash
## --------------------------------------
## 2. Start the Spark cluster workers
## --------------------------------------

# get the resource details from the Slurm job
export SPARK_WORKER_CORES=${SLURM_CPUS_PER_TASK:-1}
export SPARK_MEM=$(( ${SLURM_MEM_PER_CPU:-4096} * ${SLURM_CPUS_PER_TASK:-1} ))M
export SPARK_DAEMON_MEMORY=$SPARK_MEMsb
export SPARK_WORKER_MEMORY=$SPARK_MEM
export SPARK_EXECUTOR_MEMORY=$SPARK_MEM
export SPARK_NODES=${SLURM_NNODES}
export SPARK_TOTAL_TASKS=${SLURM_NTASKS}
export SPARK_TASKS=$(($SPARK_TOTAL_TASKS/$SPARK_NODES - 1))

# start the workers on each node allocated to the tjob
export SPARK_NO_DAEMONIZE=1
echo $MASTER_URL
echo "*****************"
#srun  --output=$SPARK_LOG_DIR/spark-%j-1-workers.out --label -N 1 -n 1 -c 8 ./start_worker_new.sh ${MASTER_URL} &
#srun  --output=$SPARK_LOG_DIR/spark-%j-2-workers.out --label -N 1 -n 1 -c 8 ./start_worker_new.sh ${MASTER_URL} &
#srun  --output=$SPARK_LOG_DIR/spark-%j-3-workers.out --label -N 1 -n 1 -c 8 ./start_worker_new.sh ${MASTER_URL} &
#srun  --output=$SPARK_LOG_DIR/spark-%j-4-workers.out --label -N 1 -n 1 -c 8 ./start_worker_new.sh ${MASTER_URL} &

echo $SPARK_NODES
echo $SPARK_TASKS

for i in $(seq 1 $SPARK_NODES )
do
  for j in $(seq 1 $SPARK_TASKS )
  do
    srun --output=$SPARK_LOG_DIR/spark-%j-$i-$j-workers.out -N 1 -n 1 -c $SPARK_WORKER_CORES ./start_worker_new.sh ${MASTER_URL} ${SPARK_WORKER_CORES} ${SPARK_MEM} &
  done
done

echo "Sleeping for 60 secs"
sleep 60
echo "Wait is over"
```
The script starts the Spark workers on each node allocated to the SLURM job. It uses `srun` to launch workers in parallel, ensuring that each worker has the necessary resources allocated.

#### Submiting the Spark Job
```bash
## --------------------------------------
## 3. Submit a task to the Spark cluster
## --------------------------------------
echo "Starting the Spark job..."
echo $(pwd)
cd  /scratch/users/k2039563/radarpipeline
echo $(pwd)
echo $(python --version)
#echo $(python3.8 --version)
python3.12 /scratch/users/k2039563/spark-test/run_radarpipeline.py $MASTER_URL
cd  /scratch/users/k2039563/spark-test/
echo $(pwd)

echo "Spark Job finished"
```

This part of the script submits a Spark job to the cluster using the `run_radarpipeline.py` script, which processes data according to the specified configuration.

#### Clean Up
```bash
## --------------------------------------
## 4. Clean up
## --------------------------------------

# stop the workers
scancel ${SLURM_JOBID}.0

# stop the master
./stop-master.sh
```
Finally, the script cleans up by stopping the Spark workers and master node.


#### start_worker_new.sh
```bash

#!/bin/bash
#module load openjdk/1.8.0_265-b01-gcc-13.2.0
module load anaconda3/2022.10-gcc-13.2.0
module load spark
eval "$(conda shell.bash hook)"
conda activate radarpipeline3.12 
export SPARK_HOME="$(find_spark_home.py)"
echo ${SPARK_HOME}
#export PATH=$PATH:/users/k2039563/.conda/envs/radarpipeline3.12/bin/spark
#export SPARK_HOME=/users/k2039563/.conda/envs/radarpipeline3.12/lib/python3.12/site-packages/pyspark

if [ -z "$SSH_AUTH_SOCK" ] ; then
  eval `ssh-agent -s`
  ssh-add
fi

MASTER_URL=$1
echo $MASTER_URL
$SPARK_WORKER_CORES=$2
$SPARK_MEM=$3
echo $SPARK_WORKER_CORES
echo $SPARK_MEM

spark-class org.apache.spark.deploy.worker.Worker $MASTER_URL -c $SPARK_WORKER_CORES -m $SPARK_MEM
```

This script starts a Spark worker node, connecting it to the specified master URL and allocating the defined resources (CPU cores and memory) for the worker.

