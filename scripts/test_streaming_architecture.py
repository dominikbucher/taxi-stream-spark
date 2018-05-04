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

    # Set the number of workers (spark.executor.instances system property).

    # Submit the job to the master (How do we collect data? Can the master dump everything after it ran for
    # a minute or so?).

    # Notify the script user of progress.
    pass


for architecture in ('BasePartition', 'Cartesian', 'SpatialIndex'):
    for batch_time in range(100, 10000, 500):
        for taxi_throughput in range(1000, 1000000, 1000):
            for client_throughput in range(1, 100000, 100):
                for num_workers in range(1, 10):
                    test_application(architecture, batch_time, taxi_throughput, client_throughput, num_workers)