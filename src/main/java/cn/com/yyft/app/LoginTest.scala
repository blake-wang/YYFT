package cn.com.yyft.app

import org.apache.log4j.Logger

/**
  * Created by JSJSB-0071 on 2016/11/21.
  */
object LoginTest {
  val logger = Logger.getLogger(LoginTest.getClass)

  def main(args: Array[String]) {

    /*val sparkConf = new SparkConf().setAppName("LoginTest").setMaster("local[2]")
     val sparkContext = new SparkContext(sparkConf)
     val ssc = new StreamingContext(sparkContext, Seconds(2)) //Minutes(3) Seconds

     val topicsSet = "active".split(",").toSet

     val kafkaParams = Map[String, String]("metadata.broker.list" -> "192.168.20.176:9092,192.168.20.177:9092,192.168.20.76:9092")

     val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
       ssc, kafkaParams, topicsSet)

     val valueMessageDstream = messages.map(_._2)
     valueMessageDstream.print()

     ssc.start()
     ssc.awaitTermination()*/
    println("aBCd".toUpperCase())
  }


}
