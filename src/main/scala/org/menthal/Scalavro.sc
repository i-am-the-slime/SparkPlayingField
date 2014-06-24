import com.gensler.scalavro.types.AvroType
import org.apache.spark.SparkContext
import org.menthal.EventData

println(AvroType[EventData].schema().prettyPrint)

