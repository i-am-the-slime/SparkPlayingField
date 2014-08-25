import sbtassembly.Plugin.AssemblyKeys._
import scala.io.Source.fromFile

net.virtualvoid.sbt.graph.Plugin.graphSettings

val configPath = "conf/sshconfig"

name := "sparkplayingfield"

version := "0.1"

scalaVersion := "2.10.4"


resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "spray" at "http://repo.spray.io/"

//seq( sbtavro.SbtAvro.avroSettings : _*)

libraryDependencies ++= Seq( //Dates and Times
  "org.joda" % "joda-convert" % "1.6"
  ,"joda-time" % "joda-time" % "2.3"
)



libraryDependencies += "io.spray" %%  "spray-json" % "1.2.6" //JSON

libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.0.6" //Monads

libraryDependencies += "com.twitter" % "parquet-avro" % "1.5.0" //Columnar Storage for Hadoop

libraryDependencies += "com.twitter" %% "algebird-core" % "0.6.0" //Monoids

libraryDependencies += "com.twitter" % "parquet-avro" % "1.5.0" //Parquet

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test" //Testing

libraryDependencies += "com.julianpeeters" % "avro-scala-macro-annotations_2.10" % "0.1"


libraryDependencies += ("com.gensler" %% "scalavro" % "0.6.2").
                            exclude("ch.qos.logback", "logback-classic")


libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "2.4.0" % "provided" ,
  ("org.apache.spark" %% "spark-core" % "1.0.0").
 // ("org.apache.spark" %% "spark-core" % "1.0.1" % "provided").
    exclude("log4j", "log4j").
    exclude("org.mortbay.jetty", "servlet-api").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-collections", "commons-collections").
    exclude("com.esotericsoftware.minlog", "minlog")
)


scalacOptions in (Compile,doc) ++= Seq("-groups", "-implicits")

