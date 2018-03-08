import java.text.SimpleDateFormat

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by bobo on 01/03/18.
  */
object WordCountPipeline {

  def main(args: Array[String]): Unit = {
    val interval = args(0)
    val kafkaURL = args(1)
    //input kafka source
    val topic = args(2)
    //input topic
    val hdfsURL = args(3)
    //output hdfs url
    val regex = args(4)
    //word filter regex
    val conf = new SparkConf()
    val ssc = new StreamingContext(conf, Seconds(interval.toInt))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaURL,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "first_group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(topic)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream.map(r => r.value())
      .flatMap(_.split(" "))
      .filter(_.matches(regex))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .foreachRDD(r => {
        val singlePartition = r.coalesce(1).sortBy(pair => pair._2, false)
        if (singlePartition.count() > 0) {
          val df = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss")
          val ts = df.format(System.currentTimeMillis)
          singlePartition.saveAsTextFile(hdfsURL + "/" + ts)
        }
      })

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}
