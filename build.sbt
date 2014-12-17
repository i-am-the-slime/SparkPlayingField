import sbtassembly.Plugin._
import AssemblyKeys._
import scala.io.Source
import scala.reflect.io.File
import scala.util.parsing.json.JSON

net.virtualvoid.sbt.graph.Plugin.graphSettings

name := "sparkplayingfield"

version := "0.1"

scalaVersion := "2.10.4"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "spray" at "http://repo.spray.io/"

resolvers += "jcenter" at "http://jcenter.bintray.com"

//javaHome := Some(file("/System/Library/Java/JavaVirtualMachines/1.6.0.jdk/Contents/Home"))

resolvers += Resolver.url(
  "bintray-menthal-models",
  url("http://dl.bintray.com/i-am-the-slime/maven"))

val excludeJBossNetty = ExclusionRule(organization = "org.jboss.netty")
//val excludeIONetty = ExclusionRule(organization = "io.netty")
val excludeEclipseJetty = ExclusionRule(organization = "org.eclipse.jetty")
val excludeMortbayJetty = ExclusionRule(organization = "org.mortbay.jetty")
val excludeAsm = ExclusionRule(organization = "org.ow2.asm")
val excludeOldAsm = ExclusionRule(organization = "asm")
val excludeCommonsLogging = ExclusionRule(organization = "commons-logging")
val excludeSLF4J = ExclusionRule(organization = "org.slf4j")
//val excludeScalap = ExclusionRule(organization = "org.scala-lang", artifact = "scalap")
val excludeHadoop = ExclusionRule(organization = "org.apache.hadoop")
//val excludeCurator = ExclusionRule(organization = "org.apache.curator")
//val excludePowermock = ExclusionRule(organization = "org.powermock")
//val excludeFastutil = ExclusionRule(organization = "it.unimi.dsi")
//val excludeJruby = ExclusionRule(organization = "org.jruby")
//val excludeThrift = ExclusionRule(organization = "org.apache.thrift")
val excludeServletApi = ExclusionRule(organization = "javax.servlet", artifact = "servlet-api")
//val excludeJUnit = ExclusionRule(organization = "junit")

libraryDependencies ++= Seq( //Dates and Times
  "org.joda" % "joda-convert" % "1.6"
  ,"joda-time" % "joda-time" % "2.3"
)

libraryDependencies += "org.menthal" % "menthal-models_2.10" % "0.12"

libraryDependencies += "io.spray" %%  "spray-json" % "1.2.6" //JSON

//libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.0.6" //Monads

libraryDependencies += ("com.twitter" % "parquet-avro" % "1.5.0") //Columnar Storage for Hadoop

libraryDependencies += "com.twitter" % "parquet-avro" % "1.5.0" //Columnar Storage for Hadoop

libraryDependencies += "com.twitter" %% "algebird-core" % "0.8.1" //Monoids

libraryDependencies += ("com.twitter" %% "chill-bijection" % "0.4.0").
  exclude("com.esotericsoftware.minlog", "minlog")

libraryDependencies += "com.twitter" % "chill-avro" % "0.4.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.0" % "test" //Testing

libraryDependencies += "org.mortbay.jetty" % "servlet-api" % "3.0.20100224"

libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.6.0-RC0" % "test"

 //libraryDependencies += ("org.apache.spark" %% "spark-sql" % "1.0.1") //Sql queries on spark shit

val hadoopExcludes = List(excludeJBossNetty, excludeEclipseJetty, excludeMortbayJetty, excludeAsm,
  excludeCommonsLogging, excludeSLF4J, excludeOldAsm, excludeServletApi)

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "2.5.0" % "provided"  excludeAll(hadoopExcludes:_*),
  "org.apache.spark" %% "spark-core" % "1.1.0" % "provided"  excludeAll excludeHadoop
  //  exclude("log4j", "log4j").
  //  exclude("commons-beanutils", "commons-beanutils").
  //  exclude("commons-beanutils", "commons-beanutils-core").
  //  exclude("commons-collections", "commons-collections").
  //  exclude("com.esotericsoftware.minlog", "minlog")
)

parallelExecution in test := false

fork in Test := true

addCompilerPlugin("org.scalamacros" % "paradise" % "2.0.0" cross CrossVersion.full)

scalacOptions in (Compile,doc) ++= Seq("-groups", "-implicits")
