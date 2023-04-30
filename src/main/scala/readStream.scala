//import org.apache.spark.SparkConf
//import org.apache.spark.streaming._
//import org.apache.spark.streaming
//
//object readStream
//val conf = new SparkConf().setAppName("KafkaStreamingApp")
//val ssc = new StreamingContext(conf, Seconds(5))
//
//val kafkaParams = Map[String, Object](
//  "bootstrap.servers" -> "localhost:9092",
//  "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
//  "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
//  "group.id" -> "my-group",
//  "auto.offset.reset" -> "earliest"
//)
//
//val topics = Array("my-topic")
//
//val stream = KafkaUtils.createDirectStream[String, String](
//  ssc,
//  LocationStrategies.PreferConsistent,
//  ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
//)
//
//stream.foreachRDD { rdd =>
//  rdd.foreach { record =>
//    println(record.key() + ": " + record.value())
//  }
//}
//
//ssc.start()
//ssc.awaitTermination()
