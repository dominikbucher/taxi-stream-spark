# Taxi Stream Application

The goal of this appication is to evaluate a number of streaming architectures for the automated processing of taxi data, in particular matching client requests with free taxis.

The application works together with a streaming counterpart that provides taxi data / updates in real-time.

### Deployment on Local Cluster

Start the master.
`C:\Spark\spark-2.3.0-bin-hadoop2.7\bin\spark-class org.apache.spark.deploy.master.Master`

Start a worker.
`C:\Spark\spark-2.3.0-bin-hadoop2.7\bin\spark-class org.apache.spark.deploy.worker.Worker spark://129.132.127.239:7077`

Start the driver.
For this, you need to remove the .master("") line in the driver program, as well as have a HDFS running.
To build the jar in Intellij, go to Project Structure and Choose "From modules with dependencies", then "COPY libraries"!
```
cd C:\Data\Programming\Teaching\taxi-streaming
C:\Spark\spark-2.3.0-bin-hadoop2.7\bin\spark-submit --class "ethz.TaxiStreamProcessor" --master local[4] classes\artifacts\taxi_streaming_jar\taxi-streaming.jar
C:\Spark\spark-2.3.0-bin-hadoop2.7\bin\spark-submit --class "ethz.TaxiStreamProcessor" --master spark://129.132.127.239:7077 classes\artifacts\taxi_streaming_jar\taxi-streaming.jar
```
