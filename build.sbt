import sbtassembly.Plugin.AssemblyKeys._
import scala.io.Source.fromFile

net.virtualvoid.sbt.graph.Plugin.graphSettings

val configPath = "conf/sshconfig"

name := "sparkplayingfield"

version := "0.1"

scalaVersion := "2.10.4"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "spray" at "http://repo.spray.io/"

libraryDependencies ++= Seq( //Dates and Times
  "org.joda" % "joda-convert" % "1.6"
  ,"joda-time" % "joda-time" % "2.3"
)

com.twitter.scrooge.ScroogeSBT.newSettings

libraryDependencies ++= Seq( //Thrift serialization and Scrooge Scala Addons
  "org.apache.thrift" % "libthrift" % "0.8.0",
  "com.twitter" %% "scrooge-core" % "3.3.2",
  "com.twitter" %% "finagle-thrift" % "6.5.0"
)

libraryDependencies += "io.spray" %%  "spray-json" % "1.2.6" //JSON

libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.0.6" //Monads

libraryDependencies += "com.twitter" % "parquet-avro" % "1.5.0" //Columnar Storage for Hadoop

libraryDependencies += "com.twitter" %% "algebird-core" % "0.6.0" //Monoids

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test" //Testing

libraryDependencies += ("com.gensler" %% "scalavro" % "0.6.2").
                            exclude("ch.qos.logback", "logback-classic")

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "2.4.0" % "provided" ,
  ("org.apache.spark" %% "spark-core" % "1.0.0").
    exclude("log4j", "log4j").
    exclude("org.mortbay.jetty", "servlet-api").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-collections", "commons-collections").
    exclude("com.esotericsoftware.minlog", "minlog")
)

val scpTask = TaskKey[String]("scp", "Copies assembly jar to remote location")

traceLevel in scpTask := -1

scalacOptions in (Compile,doc) ++= Seq("-groups", "-implicits")

scpTask <<= (assembly, streams) map { (asm, s) =>
  if(new java.io.File(configPath).exists){
    val account = fromFile(configPath).mkString
    val local = asm.getPath
    val remote = account + ":" + asm.getName
    s.log.info(s"Copying: $local -> $account:$remote")
    println("To run on the remote server copy, paste, adjust and run this: ")
    println("=============================")
    println("ssh " + account)
    println("=============================")
    println("cd spark")
    println("bin/spark-class org.apache.spark.deploy.yarn.Client \\")
    println("--jar /home/hduser/"+asm.getName+" \\")
    println("--args yarn-standalone \\")
    println("--master-memory 512M \\")
    println("--worker-memory 512M \\")
    println("--num-workers 1 \\")
    println("--worker-cores 1 \\")
    s.log.error("--class org.menthal.CLASSNAME")
    println("=============================")
    Process(s"rsync $local $remote") !! s.log
  }
  else {
    sys.error(s"$configPath not found.\n" +
    s"Please make sure you have password-less access to the hadoop/spark machine.\n" +
    s"Then create $configPath in the project root.\n" +
    "The line should contain nothing but the host (e.g. hduser@hd)\n")
    ""
  }
}
