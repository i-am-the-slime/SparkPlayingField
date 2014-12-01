import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.joda.time.DateTime
import org.menthal.model.events.{CCAppInstall, AppInstall}
import spray.json.{JsArray, JsValue}
import scala.language.implicitConversions
import spray.json.DefaultJsonProtocol._
import spray.json._
import org.menthal.model.events.Implicits._
import scala.io.Source
import scala.reflect.io.File
import scala.util.parsing.json.JSON
import org.menthal.io.postgres.PostgresDump._

//val test = new AppInstall(1,2,3,"appName", "appSausage")
//
//
//test match {
//  case x:CCAppInstall => println("beer")
//}

import org.menthal.aggregations.tools.AppSessionMonoid._
val str1 = "[\"1408467826794\",\"28\",\"Menthal\",\"open.menthal\"]"
val str2 = "\"[\\\"Facebook\\\",\\\"com.facebook.katana\\\"]\""
val tenten = """"[\"YouTube\",\"com.google.android.youtube\"]""""
val tentene2 = """"[\\"System     Android\\",\\"android\\",2]""""
val tententen = """"[\\"Xperia\\u2122      \\u2014 strona  g\\u0142\\u00f3wna\\",\\"com.sonyericsson.home/com.sonyericsson.home.HomeActivity\\",\\"\\"]""""
def parsedJSONDataArrayAsList(data: String): List[String] = {
  data.parseJson.convertTo[List[String]]
}

def parsedJSONDataAsList(data: String): List[String] = {
  if (data.startsWith("\""))
    data.substring(1, data.length() - 1).replace("\\", "").parseJson.convertTo[List[String]]
  else
    data.parseJson.convertTo[List[String]]
}
def parsedJSONDataAsStringList(data: String): List[String] = {
  if (data.startsWith("\""))
    data.substring(1, data.length() - 1).replace("\\", "").parseJson.convertTo[List[String]]
  else
    data.parseJson.convertTo[List[String]]
}
val boris = parsedJSONDataAsStringList(""""[\\"System	Android\\",\\"android/com.android.internal.policy.impl.KeyguardViewManager$KeyguardViewHost\\",\\"\\"]"""")
val line = """191824	1	2013-05-27 06:28:31.477+02	32	"[\\"System	Android\\",\\"android/com.android.internal.policy.impl.KeyguardViewManager$KeyguardViewHost\\",\\"\\"]"
             |""".stripMargin

val rawData = line.split("\t")
val l = rawData.length
val sheeet = rawData(4)
val shit = getEvent(rawData(0), rawData(1), rawData(2), rawData(3), rawData(4))
//val v1 = parsedJSONDataAsStringList(tentene2)
val v2 = parsedJSONDataAsStringList(tenten)
val v3 = parsedJSONDataAsStringList(tententen)


