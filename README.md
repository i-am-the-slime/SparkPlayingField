SparkPlayingField
=================

To build run

sbt assembly

Copy the file in target/...assembly..jar to the server.

Ssh into the server and rename the file to maderfaker.jar

To run use this:

bin/spark-submit   --class org.menthal.NewAggregations --master yarn-cluster --executor-memory 500M --num-executors 3  ../maderfaker.jar yarn-cluster hdfs:///user/hduser/events.small