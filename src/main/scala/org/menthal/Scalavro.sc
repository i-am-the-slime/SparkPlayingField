import java.io.ByteArrayOutputStream

import com.gensler.scalavro.types.AvroType
import org.menthal.model.events.Event
import org.menthal.model.events.EventData.{EventData, ScreenOff}

val event = Event(12, 12, 12, ScreenOff())
//val baos = new ByteArrayOutputStream
val hey = AvroType[Event].io.writeJson(event) + "hohoho"
//AvroType[Event].io.write(event, baos)
//baos.toByteArray

//def getLocalSparkContext: SparkContext = {
//  val conf = new SparkConf()
//    .setMaster("local")
//    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    .set("spark.kryo.registrator", "org.menthal.model.serialization.MenthalKryoRegistrator")
//    .setAppName("NewAggregationsSpec")
//    .set("spark.executor.memory", "1g")
//  val sc = new SparkContext(conf)
//  sc
//}
//val sc = getLocalSparkContext
//val smsType = AvroType[SmsReceived]
//val io:AvroTypeIO[SmsReceived] = smsType.io
//val filePath = "/Users/mark/Desktop/test"
//val outputStream = new FileOutputStream(filePath)
//io.write(new SmsReceived("contacthash", 2), outputStream)
//val inputStream = new FileInputStream(filePath)
//val Success(readResult) = io read inputStream

