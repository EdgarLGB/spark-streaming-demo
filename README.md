# spark-streaming-demo

Run "sbt clean assembly" to package the project.

# Prerequisites
  1. Install sbt
  
# How to run the demo  
  1. Run "sbt clean assembly" to build and package the project
  2. Copy the generated jar onto HDFS
  3. Start the zookeeper by running "bin/zookeeper-server-start.sh config/zookeeper.properties"
  4. Start the kafka server by running "bin/kafka-server-start.sh config/server.properties"
  5. Submit the application with the given script
  6. Upload a log file into the "data/logs" directory of HDFS
  7. You can see the result saved in "data/result" directory of HDFS after some time
