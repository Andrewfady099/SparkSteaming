import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.time.Instant
import scala.io.Source
import scala.math.Ordered.orderingToOrdered

object FileToKafka {
  def main(args: Array[String]): Unit = {
    val filePath = "/home/ejada/Downloads/Transactions"

    val topic = "transactions"

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    var InitialTime=Instant.now()

    for (line <- Source.fromFile(filePath).getLines) {

      var transactionTimeStamp=InitialTime.plusMillis(line.split(',')(2).toLong)
      while (Instant.now()<transactionTimeStamp)
      {
      }
      val record = new ProducerRecord[String, String](topic,line.split(',')(0)+","+line.split(',')(1)+","+transactionTimeStamp.toString)
      producer.send(record)
    }


    producer.close()
  }
}
