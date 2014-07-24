[![Build Status](https://travis-ci.org/i-am-the-slime/SparkPlayingField.svg?branch=master)](https://travis-ci.org/i-am-the-slime/SparkPlayingField)

SparkPlayingField
=================

To build run

Add % provided to spark dependency before you submit it to the cluster.
example:
 ("org.apache.spark" %% "spark-core" % "1.0.0" % "provided").

sbt assembly

Copy the file in target/...assembly..jar to the server.

Ssh into the server and rename the file to maderfaker.jar

To run use this:

bin/spark-submit   --class org.menthal.NewAggregations --master yarn-cluster --executor-memory 500M --num-executors 3  ../maderfaker.jar yarn-cluster hdfs:///user/hduser/events.small