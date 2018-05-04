import platform
import subprocess
from shutil import copyfile
import os
import sys

# To set up the Spark master, we follow almost the same procedure as for a worker. 
# TODO Does this have to include an OSRM instance as well? How much can run on the master?


def start_spark_master(directory):
    cmd_start_master = r'C:\Spark\spark-2.3.0-bin-hadoop2.7\bin\spark-class org.apache.spark.deploy.master.Master'
    subprocess.call(cmd_start_master, shell=True, cwd=directory)


start_spark_master('dir')