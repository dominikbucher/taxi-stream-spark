# Automated Testing Suite

To automate workflows and ensure reproducability (up to some degree), all testing should be automated.


## General Description
The idea here is more or less as follows (given by the fact that our lab PCs only run Windows as for now, this might change by the end of May):
1. Log in on multiple PCs, run set_up_master.py on one of them. This is then the master (the set_up_master.py script optimally outputs the IP and port of the Spark instance so that the other scripts can be adjusted accordingly).
2. Run set_up_worker.py on all the other PCs (maybe depending on the test we're running this could be different numbers). These will download an OSRM file and host OSRM locally, so that we can access it very quickly (no network delays). In addition, they'll add themselves to the master node / Spark system.
3. Run test_streaming_architecture.py on the master node. This will automatically vary parameters, and run the system with the different parameters. This will *also* run the streaming component locally, and restart it for each new test run so that we have (mostly) the same testing conditions.
4. The streaming application (or the test_streaming_architecture.py) will dump statistics to a file (e.g., with name `architecture_batch_time_taxi_throughput_client_throughput_num_workers.csv`) which we (hopefully) can easily import in Python / R or whatever plot / stats tool later. This will be used to find relationships between parameters and execution times. 



## TODO
* The parameter space is probably too huge now, assuming that we let each test run for around 30 seconds or so. But this can easily be reduced.