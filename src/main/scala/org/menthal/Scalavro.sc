import java.io.{FileInputStream, FileOutputStream, OutputStreamWriter, File}
import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.AvroType
import org.apache.spark.{SparkConf, SparkContext}
import org.menthal.model.events.EventData
import org.menthal.model.events.EventData._
import scala.reflect.runtime.universe._
import spray.json.DefaultJsonProtocol._
import spray.json._
import scala.util.Success

def getLocalSparkContext: SparkContext = {
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("NewAggregationsSpec")
    .set("spark.executor.memory", "1g")
  val sc = new SparkContext(conf)
  sc
}

val sc = getLocalSparkContext

val smsType = AvroType[SmsReceived]

val io:AvroTypeIO[SmsReceived] = smsType.io

val filePath = "/Users/mark/Desktop/test"

val outputStream = new FileOutputStream(filePath)

io.write(new SmsReceived("hitler", 2), outputStream)

val inputStream = new FileInputStream(filePath)

val Success(readResult) = io read inputStream
