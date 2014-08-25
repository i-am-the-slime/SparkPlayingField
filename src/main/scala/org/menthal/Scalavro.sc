import org.joda.time.DateTime
import org.menthal.model.events.{CCAppInstall, AppInstall}
import scala.language.implicitConversions
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
val a = List(1,2,3)
println(a)
