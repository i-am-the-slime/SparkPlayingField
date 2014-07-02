import com.gensler.scalavro.types.AvroType
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import org.menthal.model.events.{Alpha, Gamma, Event}
import org.menthal.model.events.EventData.{EventData, ScreenOff}
val event = Gamma(1.2)
val baos = new ByteArrayOutputStream
AvroType[Alpha].io.write(event, baos)
val in = new ByteArrayInputStream(baos.toByteArray)
AvroType[Alpha].io.read(in)

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

