import com.gensler.scalavro.types.AvroType
import java.io.{File, ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.avro.Schema.Parser
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericRecord, GenericDatumReader}

//val infile = new File("/Users/mark/Documents/SparkPlayingField/src/test/resources/shitisasshitdoes.avsc")
val infile = new File("/Users/mark/Documents/SparkPlayingField/src/test/resources/AvroTypeProviderTest16.avro")
val schemar = infile.getName.split("\\.").last match {
  case "avro" =>
    val gdr = new GenericDatumReader[GenericRecord]
    val dfr = new DataFileReader(infile, gdr)
    dfr.getSchema
  case "avsc" =>
    new Parser().parse(infile)
  case _ => throw new Exception("Invalid file ending. Must be .avsc for plain text json files and .avro for binary files.")
}

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

