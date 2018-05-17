# Taxi Stream Application

The goal of this appication is to evaluate a number of streaming architectures for the automated processing of taxi data, 
in particular matching client requests with free taxis.

The application works together with a streaming counterpart that provides taxi data / updates in real-time.


### Deployment on Local Cluster

Start the master.
`C:\Spark\spark-2.3.0-bin-hadoop2.7\bin\spark-class org.apache.spark.deploy.master.Master`

Start a worker.
`C:\Spark\spark-2.3.0-bin-hadoop2.7\bin\spark-class org.apache.spark.deploy.worker.Worker spark://129.132.127.239:7077`

Start the driver.
For this, you need to remove the .master("") line in the driver program, as well as have a HDFS running.
Building the jar in Intellij leads to errors, instead it's easier to use Maven. Copy the following into `pom.xml`:
```xml
<build>
  <plugins>
    <plugin>
      <artifactId>maven-assembly-plugin</artifactId>
      <configuration>
        <archive>
          <manifest>
            <mainClass>fully.qualified.MainClass</mainClass>
          </manifest>
        </archive>
        <descriptorRefs>
          <descriptorRef>jar-with-dependencies</descriptorRef>
        </descriptorRefs>
      </configuration>
    </plugin>
  </plugins>
</build>
```

And build the file using:
```
mvn clean compile assembly:single
```
This will put the .jar file into the `target/` folder. You 
To build the jar in Intellij, go to Project Structure and Choose "From modules with dependencies", then "COPY libraries"!
```
cd C:\Data\Programming\Teaching\taxi-streaming
C:\Spark\spark-2.3.0-bin-hadoop2.7\bin\spark-submit --class "ethz.TaxiStreamProcessor" --master local[4] classes\artifacts\taxi_streaming_jar\taxi-streaming.jar
C:\Spark\spark-2.3.0-bin-hadoop2.7\bin\spark-submit --class "ethz.TaxiStreamProcessor" --master spark://129.132.127.239:7077 classes\artifacts\taxi_streaming_jar\taxi-streaming.jar
```

# Running the Application on the Lab Cluster

Everything is in the folder C:\sprk.

## Setting up Hadoop / HDFS

We use hadoop-3.0.2 for now (as the hadoop utils files from https://github.com/steveloughran/winutils are only available for hadoop-3.0.0 yet; they should work with hadoop-3.x.x though). 

Follow the instructions on https://wiki.apache.org/hadoop/Hadoop2OnWindows, even though 
they might be a bit outdated. In particular, edit the following files in etc/hadoop (using the default values for a local filesystem; this will be mounted to C:/tmp/hadoop-labadmin/dfs/data/current):
```
core-site.xml
hdfs-site.xml
mapred-site.xml
yarn-site.yml
slaves
workers (same as slaves)
```

Then format the (distributed) file system with 
```
C:\sprk\hadoop-3.0.2\hadoop-3.0.2\bin\hdfs.cmd namenode -format
```

and start it afterwards with
```
C:\sprk\hadoop-3.0.2\hadoop-3.0.2\sbin\start-dfs.cmd
```

The port can be retrieved using:
```
C:\sprk\hadoop-3.0.2\hadoop-3.0.2\bin\hdfs.cmd getconf -confKey fs.defaultFS
```

Some more things that are important for configuration:
* Set all environment variables by running `C:\sprk\hadoop-3.0.2\hadoop-3.0.2\etc\hadoop\hadoop-env.cmd`.
* Set HADOOP_HOME (environment variable) to C:\sprk\hadoop-3.0.2\hadoop-3.0.2. It will read the winutils.exe from there (bin/ subfolder).
* Disable firewall completely...
* Copy all Windows binaries (get them from https://github.com/steveloughran/winutils) to C:\sprk\hadoop-3.0.2\hadoop-3.0.2\bin.

The default directory for the HDFS is `C:\tmp`.

## Postgres

The password for the postgres database user is password.

## Spark
Start the master using 
```
C:\sprk\spark-2.3.0-bin-hadoop2.7\bin\spark-class org.apache.spark.deploy.master.Master
```

And the workers using:
```
C:\sprk\spark-2.3.0-bin-hadoop2.7\bin\spark-class org.apache.spark.deploy.worker.Worker --master spark://129.132.26.208:7077
```

Once they're running you can submit a job using:
```
C:\sprk\spark-2.3.0-bin-hadoop2.7\bin\spark-submit.cmd --class "ch.ethz.gis.TaxiStreamProcessor" --master spark://129.132.26.208:7077 --executor-memory 20G C:\sprk\Spark-Taxi-Streaming-0.0.1-jar-with-dependencies.jar cartesianKnn 2000 true C:\sprk 127.0.0.1:5432/taxi-streaming hdfs://129.132.26.208:19000/checkpoint
```

Important things here:
* The executor memory must be high, otherwise we'll run in an error where the RDD partitions cannot be created or so.
* The HDFS URL must not be 0.0.0.0, as the workers directly write there as well.


## Go Streaming Service
(TODO: Seems to have trouble streaming when a .exe).
Adjust the `config.json`, then run it using `taxistream.exe config.json`. 
