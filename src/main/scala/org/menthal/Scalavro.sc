import java.io.File
import com.gensler.scalavro.types.AvroType
import org.menthal.model.events.EventData
import org.menthal.model.events.EventData._
import scala.reflect.runtime.universe._
import spray.json.DefaultJsonProtocol._
import spray.json._

//println(AvroType[SmsReceived].schema().prettyPrint)
val data = "\"[\\\\\"719b886597f64e0c48c087848c676fde59a5a61cd3ee5940461ce3b1c0d9b602b706427d532a24badf32ec499de09a8098ad8e0e56aa0bfef1facea603ac5a09\\\\\",17,1]\""
val guy = data.substring(1, data.length()-1).replace("\\","").parseJson.asInstanceOf[JsArray].elements(1).convertTo[Int]

val nuy = data.substring(1, data.length()-1).replace("\\", "").parseJson.asInstanceOf[(String, Int, Int)]


