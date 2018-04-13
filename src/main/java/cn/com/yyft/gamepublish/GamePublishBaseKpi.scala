package cn.com.yyft.gamepublish

import cn.com.yyft.utils._
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by 苏孟虎 on 2016/8/24.
  * 对游戏发布数据报表统计-基本指标
  */
object GamePublishBaseKpi {

  val logger = Logger.getLogger(GamePublishBaseKpi.getClass)

  var arg = "10"

  def main(args: Array[String]) {

    arg = args(0)
    val ssc = StreamingContext.getOrCreate(PropertiesUtils.getRelativePathValue("checkpointdir"), functionToCreateContext _)
    ssc.start()
    ssc.awaitTermination()
  }

  def functionToCreateContext(): StreamingContext = {
    val Array(brokers, topics) = Array(PropertiesUtils.getRelativePathValue("brokers"), PropertiesUtils.getRelativePathValue("topic"))
    val sparkConf = new SparkConf().setAppName("GamePublishBaseKpi")
      .set("spark.sql.shuffle.partitions", PropertiesUtils.getRelativePathValue("spark_sql_shuffle_partitions"))
    //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkContext = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkContext, Minutes(Integer.parseInt(arg))) //Minutes(3) Seconds

    val topicsSet = topics.split(",").toSet

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "fetch.message.max.bytes" -> "1048576000",
      "group.id" -> "GamePublishBaseKpi")
    //从kafka中获取所有游戏发行日志数据
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val valueMessageDstream = messages.map(_._2)
    //val broadcastRedis = sparkContext.broadcast(new MyRedisClient());
    //多线程同步task
    valueMessageDstream.foreachRDD(rdd => {
      //rdd 不 能空
      if (rdd.count() > 0) {
        val sc = rdd.sparkContext
        val sqlContext = SQLContextSingleton.getInstance(sc)

        StreamingUtils.getPubGameFullData(sc, sqlContext)
        StreamingUtils.convertPubGameLogsToDfTmpTable(rdd, sqlContext)

        StreamingUtils.getRegiFullData(sc, sqlContext)

        StreamingUtils.getOrderFullData(sc, sqlContext)

        StreamingUtils.convertLogsToDfTmpTable(rdd, sqlContext)

        StreamingUtils.gameConsumer(sqlContext)

        val result = sqlContext.sql("select publish_time, game_id,expand_channel ,sum(show_num) show_num,sum(download_num) download_num,sum(active_num) active_num" +
          ",sum(pay_account_num) pay_account_num,sum(pay_money) pay_money,sum(regi_account_num) regi_account_num,sum(request_num) request_num,sum(adpage_click_num) adpage_click_num " +
          "from gameConsumer group by game_id,expand_channel,publish_time")
        result.rdd.foreachPartition(rows => {
          //创建jedis客户端
          val redisClient = JedisUtils.getPool()
          val jedis = redisClient.getResource

          val conn = MySqlUtils.getConn()
          val connFx = MySqlUtils.getFxConn()
          val statement = conn.createStatement

          val sqlText = "insert into bi_gamepublic_basekpi(publish_time,game_id,parent_channel,child_channel,ad_label,show_num,download_num,pay_account_num," +
            "pay_money,regi_account_num,request_num,adpage_click_num," +
            "parent_game_id,os,medium_account,promotion_channel,promotion_mode,head_people,group_id) " +
            "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) " +
            "on duplicate key update show_num=show_num+?,download_num=download_num+?," +
            "pay_account_num=?,pay_money=pay_money+?," +
            "regi_account_num=regi_account_num+?,request_num=request_num+?,adpage_click_num=adpage_click_num+?"

          val sqlHourText = "insert into bi_gamepublic_base_day_kpi(publish_date,child_game_id,medium_channel," +
            "ad_site_channel,pkg_code,show_num,download_num,request_click_num,adpage_click_num,pay_money,regi_account_num," +
            "parent_game_id,os,medium_account,promotion_channel,promotion_mode,head_people,group_id)" +
            " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" +
            " on duplicate key update show_num=show_num+?,download_num=download_num+?,request_click_num=request_click_num+?,adpage_click_num=adpage_click_num+?," +
            "regi_account_num=regi_account_num+?"


          val params = new ArrayBuffer[Array[Any]]()
          val paramsHour = new ArrayBuffer[Array[Any]]()
          for (insertedRow <- rows) {
            val channelArray = StringUtils.getArrayChannel(insertedRow.get(2).toString)
            if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {

              //获取redis相关数据
              val game_id = insertedRow.getInt(1)
              val pkg_code = channelArray(2)
              val order_date = insertedRow.getString(0).split(" ")(0)
              val redisValue = StreamingUtils.getRedisValue(game_id, pkg_code, order_date, jedis, connFx)

              params.+=(Array[Any](insertedRow.get(0), insertedRow.get(1), channelArray(0), channelArray(1), channelArray(2), insertedRow.get(3),
                insertedRow.get(4), insertedRow.get(6),
                insertedRow.get(7), insertedRow.get(8), insertedRow.get(9), insertedRow.get(10),
                redisValue(0), redisValue(1), redisValue(2), redisValue(3), redisValue(4), redisValue(5), redisValue(6),
                insertedRow.get(3), insertedRow.get(4),
                insertedRow.get(6), insertedRow.get(7), insertedRow.get(8), insertedRow.get(9), insertedRow.get(10)))

              if (!(insertedRow.get(3).toString == "0" &&
                insertedRow.get(4).toString == "0" &&
                insertedRow.get(9).toString == "0" &&
                insertedRow.get(10).toString == "0" &&
                insertedRow.get(8).toString == "0")) {
                paramsHour.+=(Array[Any](insertedRow.getString(0).split(" ")(0), insertedRow.get(1), channelArray(0), channelArray(1), channelArray(2)
                  , insertedRow.get(3), insertedRow.get(4), insertedRow.get(9), insertedRow.get(10), insertedRow.get(7), insertedRow.get(8),
                  redisValue(0), redisValue(1), redisValue(2), redisValue(3), redisValue(4), redisValue(5), redisValue(6)
                  , insertedRow.get(3), insertedRow.get(4), insertedRow.get(9), insertedRow.get(10), insertedRow.get(8)))
              }
            } else {
              logger.error("推广渠道格式错误：" + insertedRow.get(2) + " - " + insertedRow.get(1))
            }
          }
          try {
            MySqlUtils.doBatch(sqlText, params, conn)
            MySqlUtils.doBatch(sqlHourText, paramsHour, conn)
          } finally {
            statement.close()
            conn.close
            connFx.close()
          }
          redisClient.returnResource(jedis)
          redisClient.destroy()
        })

      }
    })

    ssc.checkpoint(PropertiesUtils.getRelativePathValue("checkpointdir"))
    ssc
  }
}
