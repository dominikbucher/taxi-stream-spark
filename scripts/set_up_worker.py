import platform
import subprocess
from shutil import copyfile
import os
import sys

# There are several things we'd like to do on a worker:
# 1) Set up a local OSRM instance (using NYC data for taxis). This ensures that each worker can query
#    OSRM simply by using its own IP: 127.0.0.1.
#    Steps for this: 1) Download OSRM file from a central server (we can use the desktop where the streaming
#    application already runs now for this purpose). 2) Start OSRM on a certain port, let's say 9001.
# 2) Start Spark in worker mode, connecting to a master node. This should then just run in the background,
#    maybe returning us a PID how we can kill the process.

# The IP address of the PC serving all data (in particular OSRM files and the data streams).
lab_head_ip = '123.123.123.123' 

# The IP address of the Spark master.
master_ip = '123.123.123.123'


def start_osrm(directory, port, osm_file):
    # First, we check if the .osrm file already exists in the directory.
    # TODO

    # Otherwise, we download it from the lab head.
    # TODO

    # Then, we start OSRM on the correct port.
    osrm_routed = r'C:\Data\Projects\SBB-GC\OSRM-Local\2018-03-20-osrm_release\osrm-routed.exe'
    cmd_start_osrm = osrm_routed + " -p " + str(port) + " " + osm_file.split('.')[0] + ".osrm"
    subprocess.call(cmd_start_osrm, shell=True, cwd=directory)


def start_worker(directory):
    cmd_start_master = r'C:\Spark\spark-2.3.0-bin-hadoop2.7\bin\spark-class org.apache.spark.deploy.worker.Worker spark://129.132.127.239:7077'
    subprocess.call(cmd_start_master, shell=True, cwd=directory)



start_osrm('dir', 9001, 'nyc.osrm')