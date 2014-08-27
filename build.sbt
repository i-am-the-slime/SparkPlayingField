import sbtassembly.Plugin._
import AssemblyKeys._
import scala.io.{Codec, Source}
import scala.io.Source.fromFile
import scala.reflect.io.File
import scala.util.parsing.json.JSON

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

Seq( sbtavro.SbtAvro.avroSettings : _*)

(sourceDirectory in avroConfig) := new java.io.File("model/avro")

(stringType in avroConfig) := "String"


libraryDependencies += "io.spray" %%  "spray-json" % "1.2.6" //JSON

//libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.0.6" //Monads

libraryDependencies += ("com.twitter" % "parquet-avro" % "1.5.0") //Columnar Storage for Hadoop

libraryDependencies += "com.twitter" % "parquet-avro" % "1.5.0" //Columnar Storage for Hadoop

libraryDependencies += "com.twitter" %% "algebird-core" % "0.6.0" //Monoids

libraryDependencies += ("com.twitter" %% "chill-bijection" % "0.4.0").
  exclude("com.esotericsoftware.minlog", "minlog")

libraryDependencies += "com.twitter" % "chill-avro" % "0.4.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.0" % "test" //Testing



//libraryDependencies += ("org.apache.spark" %% "spark-sql" % "1.0.1") //Sql queries on spark shit

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "2.4.0" % "provided" ,
  ("org.apache.spark" %% "spark-core" % "1.0.0").
  //("org.apache.spark" %% "spark-core" % "1.0.0" % "provided").
    exclude("log4j", "log4j").
    exclude("commons-beanutils", "commons-beanutils").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("com.esotericsoftware.minlog", "minlog")
)

test in assembly := {}

fork in Test := true

addCompilerPlugin("org.scalamacros" % "paradise" % "2.0.0" cross CrossVersion.full)

scalacOptions in (Compile,doc) ++= Seq("-groups", "-implicits")

sourceGenerators in Compile += Def.task {
  val path = "./model/avro"
  val directory = File(path).toDirectory
  val nameNamespaceFields = directory.files.filter(_.extension == "avsc").toList.flatMap( file => {
    val schema = Source.fromFile(file.path).getLines().reduce(_ + _)
    val json = JSON.parseFull(schema)
    json.flatMap{
      case m:Map[String, Any] =>
        val triple = for {
          name <- m.get("name")
          namespace = m.getOrElse("namespace", "").asInstanceOf[String]
          f <- m.get("fields")
          fields = f.asInstanceOf[Seq[Map[String, String]]]
        } yield (name, namespace, fields)
        if(triple.isEmpty){
          sys.error(s"File $file is missing required fields name or fields")
          None
        }
        else {
          triple match {
            case Some((name:String, namespace:String, fields:Seq[Map[String, String]])) =>
              val fieldTuples = for{
                field <- fields
                name <- field.get("name")
                t <- field.get("type")
                tpe = t.charAt(0).toUpper + t.tail
              } yield (name, tpe)
              Some((name, namespace, fieldTuples))
            case _ => None
          }
        }
      case _ =>
        sys.error(s"Could not parse file $file")
        None
    }
  }).groupBy(_._2)
  var lines:List[String] = List()
  nameNamespaceFields.keys.map{ ns =>
    lines = s"package $ns" :: lines
    lines = "import org.apache.avro.specific.SpecificRecord" :: lines
    lines = "import org.apache.spark.Partitioner" :: lines
    lines = "import Implicits._" :: lines
    lines = "trait MenthalEvent { " :: lines
    //lines = "  def id:Long" :: lines
    lines = "  def userId:Long" :: lines
    lines = "  def time:Long" :: lines
    lines = "  def toAvro:SpecificRecord" :: lines
    lines = "}" :: lines
    val classes = nameNamespaceFields.get(ns).get
    classes.foreach{ cl =>
      val name = cl._1
      val fields = cl._3.map{case (nm:String,tp:String) => nm+":"+tp}.mkString(", ")
      lines = s"case class CC$name($fields) extends MenthalEvent" :: lines
      lines = s"{ def toAvro:$name = this }" :: lines
    }
    lines = "class EventPartitioner extends Partitioner {" :: lines
    val numberOfClasses = classes.length
    lines = s"  override def numPartitions: Int = $numberOfClasses" :: lines
    lines = "" :: lines
    lines = "  override def getPartition(key: Any): Int = {" :: lines
    lines = "    val event = key.asInstanceOf[MenthalEvent]" :: lines
    lines = "    event.toAvro match {" :: lines
    classes.zipWithIndex.foreach{ case ((className, _, _),index) =>
      lines = s"      case _:$className => $index" :: lines
    }
    lines = "    }" :: lines
    lines = "  }" :: lines
    lines = "}" :: lines
    lines = "" :: lines
    lines = "\nobject Implicits{" :: lines
    classes.foreach{ cl =>
      val name = cl._1
      val fields = cl._3.map("x."+_._1).mkString(", ")
      val getFields = cl._3.map{x =>
        val firstLetterUppercased = x._1.charAt(0).toUpper + x._1.tail
        "x.get"+firstLetterUppercased
      }.mkString(", ")
      lines = s" implicit def toCC$name(x:$name):CC$name = CC$name($getFields)" :: lines
      lines = s" implicit def to$name(x:CC$name):$name = new $name($fields)" :: lines
    }
    lines = "}" :: lines
    val path = (sourceManaged in Compile).value / "compiled_avro" / "org" / "menthal" / "model" / "events" / "FuckingName.scala"
    IO.write(path, lines.reverse.mkString("\n"))
    path
  }.toSeq
}.taskValue


