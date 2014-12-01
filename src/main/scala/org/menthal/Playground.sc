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
val tentene2 = """"[\\"Nyx\\",\\"com.menthal.nyx\\"]""""
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


val v1 = parsedJSONDataAsStringList(tentene2)
val v2 = parsedJSONDataAsStringList(tenten)


