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