import sbtassembly.Plugin.AssemblyKeys._
import scala.io.Source.fromFile

name := "simpleapp"

version := "0.1"

scalaVersion := "2.10.4"

val account = fromFile("conf/sshconfig").mkString

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "2.4.0" % "provided",
  ("org.apache.spark" %% "spark-core" % "0.9.1" % "provided").
    exclude("org.mortbay.jetty", "servlet-api").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-collections", "commons-collections").
    exclude("com.esotericsoftware.minlog", "minlog")
)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

val scpTask = TaskKey[Unit]("scp", "Copies assembly jar to remote location")

scpTask <<= assembly map { (asm) =>
  val local = asm.getPath
  val remote = account + ":" + asm.getName
  println(s"Copying: $local -> $account:$remote")
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
  println("--worker-cores 1 \\")
  println("")
  println("=============================")
  Seq("scp", local, remote) !!
}

