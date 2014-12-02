import sbtassembly.Plugin._
import AssemblyKeys._
import scala.io.Source
import scala.reflect.io.File
import scala.util.parsing.json.JSON

net.virtualvoid.sbt.graph.Plugin.graphSettings

val configPath = "conf/sshconfig"

name := "sparkplayingfield"

version := "0.1"

scalaVersion := "2.10.4"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "spray" at "http://repo.spray.io/"

//javaHome := Some(file("/System/Library/Java/JavaVirtualMachines/1.6.0.jdk/Contents/Home"))

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

//seq( sbtavro.SbtAvro.avroSettings : _*)

libraryDependencies ++= Seq( //Dates and Times
  "org.joda" % "joda-convert" % "1.6"
  ,"joda-time" % "joda-time" % "2.3"
)

Seq( sbtavro.SbtAvro.avroSettings : _*)

(sourceDirectory in avroConfig) := new java.io.File("model/avro")

(stringType in avroConfig) := "String"

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

sourceGenerators in Compile += Def.task {
  type ClassTriplet = (String, String, Seq[(String, String)])
  type AvroFileTriplet = (String, String, Seq[Map[String, Any]])
  def getNameNamespaceAndFields(m:Map[String, Any]): Option[AvroFileTriplet] = {
    for {
      name <- m.get("name")
      deeName = name.asInstanceOf[String]
      namespace = m.getOrElse("namespace", "").asInstanceOf[String]
      f <- m.get("fields")
      fields = f.asInstanceOf[Seq[Map[String, Any]]]
    } yield (deeName, namespace, fields)
  }
  def toCamelToe(s:String):String = s.charAt(0).toUpper + s.tail
  def parseAvroType(tpe:Any):String = {
    tpe match {
      case s:String =>
        toCamelToe(s)
      case complex:Map[String, Any] =>
        complex("type") match {
          case "map" =>
            val values = toCamelToe(complex("values").asInstanceOf[String])
            s"scala.collection.mutable.Map[String, $values]"
          case "array" =>
            val items = toCamelToe(complex("items").asInstanceOf[String])
            s"scala.collection.mutable.Buffer[$items]"
        }
    }
  }
  def jsonMapsToTuples(fields: Seq[Map[String, Any]]):Seq[(String, String)] =
    for{
      field <- fields
      n <- field.get("name")
      name = n.asInstanceOf[String]
      t <- field.get("type")
      tpe = parseAvroType(t)
    } yield (name, tpe)
  def processJson(m:Map[String, Any]): Option[ClassTriplet] = {
    getNameNamespaceAndFields(m) match {
      case Some((name:String, namespace:String, fields:Seq[Map[String, String]])) =>
        val fieldTuples = jsonMapsToTuples(fields)
        Some((name, namespace, fieldTuples))
      case _ => None
    }
  }
  def processFile (file:File): Option[ClassTriplet] = {
    val schema = Source.fromFile(file.path).getLines().reduce(_ + _)
    val json = JSON.parseFull(schema)
    json.flatMap{
      case m:Map[String, Any] =>
        processJson(m)
      case _ =>
        None
    }
  }
  def classTripletsFromAvroDir(avroPath:String):Seq[ClassTriplet] = {
    val directory = File(avroPath).toDirectory
    directory.files.filter(_.extension == "avsc")
      .toList
      .flatMap(processFile)
  }
  def generateImports(ns: String): List[String] = {
    List(
      s"package $ns",
      "import org.apache.avro.specific.SpecificRecord",
      //"import org.apache.spark.Partitioner",
      "import scala.collection.JavaConverters._",
      "import java.util",
      "import java.lang",
      "import scala.collection",
      "import Implicits._",
      "\n"
    )
  }
  def generateMainTrait(ns: String): List[String] ={
    List(
      "trait MenthalEvent { ",
      "  def userId:Long",
      "  def time:Long",
      "  def toAvro:SpecificRecord",
      "}\n")
  }
  def generateClasses(classes:Seq[ClassTriplet]):List[String] = {
    classes.flatMap {case (name, _, fieldsList) =>
      val fields = fieldsList.map {case (nm:String,tp:String) => nm+":"+tp}.mkString(", ")
      List(s"case class CC$name($fields) extends MenthalEvent", s"{ def toAvro:$name = this }\n")
    }.toList
  }
  def generateImplicits(classes: Seq[ClassTriplet]): List[String] = {
    val genImplicitsList = classes.flatMap { case (name, _, fields) =>
      val (scalaFields, javaFields) = fields.map { case (nme, tpe) =>
        val camelNme = toCamelToe(nme)
        tpe match {
          case li if li.startsWith("scala.collection.mutable.Buffer") =>
            val regex = "scala.collection.mutable.Buffer\\[(.*)\\]".r
            val elType = regex.findFirstMatchIn(li).get.group(1)
            (s"x.$nme.asJava", s"x.get$camelNme.asScala")
          case m if m.startsWith("scala.collection.mutable.Map") =>
            val regex = "scala.collection.mutable.Map\\[String,(.*)\\]".r
            val elType = regex.findFirstMatchIn(m).get.group(1)
            (s"x.$nme.asJava.asInstanceOf[java.util.Map[String, java.lang.$elType]]",
              s"x.get$camelNme.asScala.asInstanceOf[scala.collection.mutable.Map[String, $elType]]")
          case _ =>
            (s"x.$nme", s"x.get$camelNme")
        }
      }.unzip
      List(s" implicit def toCC$name(x:$name):CC$name = CC$name(${javaFields.mkString(", ")})",
        s" implicit def to$name(x:CC$name):$name = new $name(${scalaFields.mkString(", ")})")
    }.toList
    List("\n","object Implicits{") ::: genImplicitsList ::: List("}\n","\n")
  }
  val avroPath = "./model/avro"
  val groupedClassTriplets = classTripletsFromAvroDir(avroPath)
                                  .groupBy {case (_,namespace, _) => namespace}
  val paths = groupedClassTriplets.map {
    case (ns, classes) =>
      val genImports = generateImports(ns)
      val genTrait = generateMainTrait(ns)
      val genClasses = generateClasses(classes)
      val genImplicits = generateImplicits(classes)
      val content = genImports ::: genTrait ::: genClasses ::: genImplicits
      val path = (sourceManaged in Compile).value / "compiled_avro" / ns.replace(".","/") / "AvroScalaConversions.scala"
      IO.write(path, content.mkString("\n"))
      path
  }
  paths.toSeq
}.taskValue

