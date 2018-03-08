import java.util.Properties

import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by bobo on 01/03/18.
  */
object FilterPipeline {
  
  def main(args: Array[String]): Unit = {
    val interval = args(0)
    val directory = args(1)
    val regex = args(2)
    val kafkaUrl = args(3) //"localhost:9092"
    val outputTopic = args(4) //single topic
    
    val topics = Array(outputTopic)
    val conf = new SparkConf()
    val ssc = new StreamingContext(conf, Seconds(interval.toInt))

    val kafkaProducer: Broadcast[MySparkKafkaProducer[Array[Byte], String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", kafkaUrl)
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      ssc.sparkContext.broadcast(MySparkKafkaProducer[Array[Byte], String](kafkaProducerConfig))
    }
    
    val lines = ssc.textFileStream(directory)
    val filteredLines = lines.filter(l => l.matches(regex))
    
    //send the filtered log to a topic
    filteredLines.foreachRDD(l => {
      l.foreachPartition(p => {
        val metaData = p.map(record => kafkaProducer.value.send(topics(0), record)).toStream
        metaData.foreach(m => m.get())
      })
    })
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
