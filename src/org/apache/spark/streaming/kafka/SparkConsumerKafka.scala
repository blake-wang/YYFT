package org.apache.spark.streaming.kafka

import kafka.serializer.StringDecoder

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * Created by sunzhiwei on 2016/5/23.
 */
object SparkConsumerKafka extends Logging {
  def main(args: Array[String]) {
//AppPointTest
    val sparkConf = new SparkConf().setAppName("SparkConsumerKafka").setMaster("local").set("", "")
    // spark.streaming.kafka.maxRatePerPartition 限速
    val ssc = new StreamingContext(sparkConf, Seconds(40))
    // Create direct kafka stream with brokers and topics
    val topicsSet = "points".split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "192.168.20.81:9092,192.168.20.187:9092",
      "group.id" -> "kafkatestgrouppoints",
      "auto.offset.reset" -> "smallest"//smallest largest
    )
    val km = new KafkaManager(kafkaParams)
    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,
      kafkaParams, topicsSet)

    val tRdd = messages.transform(rdd =>{
      km.updateZKOffsets(rdd)
      rdd.map(line => line._2)
    })

    tRdd.foreachRDD(rdd => {
      val sc = rdd.sparkContext

      rdd.collect().foreach(line => {
        println("line=" +line)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}