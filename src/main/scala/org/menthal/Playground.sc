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
def parsedJSONDataArrayAsList(data: String): List[String] = {
  data.parseJson.convertTo[List[String]]
}

def parsedJSONDataStringAsList(data: String): List[String] = {
  //Data parsed from JSON into a list
  data.substring(1, data.length() - 1).replace("\\", "").parseJson.convertTo[List[String]]
}

def parsedJSONDataAsList(data: String): List[String] = {
  if (data.startsWith("\""))
    data.substring(1, data.length() - 1).replace("\\", "").parseJson.convertTo[List[String]]
  else
    data.parseJson.convertTo[List[String]]
}


val v1 = parsedJSONDataAsList(str1)

val v2 = parsedJSONDataAsList(str2)