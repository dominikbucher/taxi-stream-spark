import platform
import subprocess
from shutil import copyfile
import os
import sys

# Here, we start both the streaming, as well as the streaming application in an orchestrated way.
# I think it's best if the streaming applcation runs on the same node as well, to make sure
# we don't measure any network latency or throughput in any way (except within the cluster of course).

master_ip = '123.123.123.123'


def test_application(architecture, batch_time, taxi_throughput, client_throughput, num_workers):
    # Set the necessary parameters in the Go Streaming config.
    config = {
    '': 3000
    }

    json.from_dict(config)   
    cmd_start_master = r'go.exe'
    pid1 = subprocess.call(cmd_start_master, shell=True, cwd=directory) 

    # Set the number of workers (spark.executor.instances system property).
    sys.set_envvar('spark.streaming.blockInterval')

    # Submit the job to the master (How do we collect data? Can the master dump everything after it ran for
    # a minute or so?).
    cmd_start_master = r'spark-deploy'
    pid2 = subprocess.call(cmd_start_master, shell=True, cwd=directory) 

    # Notify the script user of progress.
    timeout(10000, kill pid1, pid2)
    pass


for architecture in ('BasePartition', 'Cartesian', 'SpatialIndex'):
    for batch_time in range(100, 10000, 500):
        for taxi_throughput in range(1000, 1000000, 1000):
            for client_throughput in range(1, 100000, 100):
                for num_workers in range(1, 10):
                    test_application(architecture, batch_time, taxi_throughput, client_throughput, num_workers)