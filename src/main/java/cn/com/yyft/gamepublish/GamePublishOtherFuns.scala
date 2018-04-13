package cn.com.yyft.gamepublish

import cn.com.yyft.utils._
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object GamePublishOtherFuns {
  val logger = Logger.getLogger(GamePublishOtherFuns.getClass)
  var arg = "10"

  def main(args: Array[String]) {
    arg = args(0)
    val ssc = StreamingContext.getOrCreate(PropertiesUtils.getRelativePathValue("otherfuncheckpointddir"), functionToCreateContext _)
    ssc.start()
    ssc.awaitTermination()
  }


  /**
    * 主程序
    *
    * @return StreamingContext
    */

  def functionToCreateContext(): StreamingContext = {
    val Array(brokers, topics) = Array(PropertiesUtils.getRelativePathValue("brokers"), PropertiesUtils.getRelativePathValue("otherfuntopics"))
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.memory.storageFraction", PropertiesUtils.getRelativePathValue("spark_memory_storageFraction"))
      .set("spark.sql.shuffle.partitions", PropertiesUtils.getRelativePathValue("spark_sql_shuffle_partitions"))
    val sparkContext = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkContext, Minutes(Integer.parseInt(arg)))
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "fetch.message.max.bytes" -> "1048576000"
      , "group.id" -> "GamePublishOtherFuns")
    //从kafka中获取所有游戏发行日志数据
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    val valueMessageDstream = messages.map(_._2)

    valueMessageDstream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val sc = rdd.sparkContext
        val sqlContext = SQLContextSingleton.getInstance(sc)
        StreamingUtils.getPubGameFullData(sc, sqlContext)
        StreamingUtils.convertPubGameLogsToDfTmpTable(rdd, sqlContext)

        //处理os数据（原GamePublishOsInfo过程）
        StreamingUtils.convertLogsToChannelDfTmpTable(rdd, sqlContext)
        val channelDF = sqlContext.sql("select publish_time,game_id,expand_channel,os,count(1) from channel " +
          "group by game_id,expand_channel,publish_time,os")
        foreachchannelPartition(channelDF);

        //处理登录数据（原GamePublishLogin过程）
        StreamingUtils.converToLoginHourDfTmpTable(rdd, sqlContext)
        val loginDf = sqlContext.sql("select DISTINCT login_time,game_id,expand_channel,game_account from login_hour")
        foreachloginPartition(loginDf);

        //处理激活广点通数据（原gamePublishActive过程）
        StreamingUtils.convertLogsToActiveDfTmpTable(rdd, sqlContext)
        val activedf = sqlContext.sql("select a.game_id,a.expand_channel,a.imie,a.update_time from active a join lastPubGame b on a.game_id=b.game_id")
        foreachactivePartition(activedf);

      }
    })

    ssc.checkpoint(PropertiesUtils.getRelativePathValue("otherfuncheckpointddir"))
    ssc
  }

  def foreachchannelPartition(channelDF: DataFrame) = {
    channelDF.foreachPartition(rows => {
      val conn = MySqlUtils.getConn()
      val statement = conn.createStatement

      val sqlText = "insert into bi_gamepublic_os_info(publish_time,game_id,parent_channel,child_channel,ad_label,os,os_sum,create_time)" +
        " values(?,?,?,?,?,?,?,?)" +
        " on duplicate key update os_sum=os_sum+?"

      val params = new ArrayBuffer[Array[Any]]()
      for (row <- rows) {
        val channelArray = StringUtils.getArrayChannel(row.getString(2))
        params.+=(Array[Any](row(0), row(1), channelArray(0), channelArray(1), channelArray(2),
          row(3), row(4), DateUtils.getNowFullDate("yyyy-MM-dd HH:mm:ss"), row(4)))
      }

      try {
        MySqlUtils.doBatch(sqlText, params, conn)
      } catch {
        case ex: Exception => {
          logger.error("=========================插入bi_gamepublic_os_info表异常：" + ex)
        }
      } finally {
        statement.close()
        conn.close
      }
    })
  }

  def foreachloginPartition(loginDf: DataFrame) = {

    loginDf.foreachPartition(rows => {
      val conn = MySqlUtils.getConn()
      val statement = conn.createStatement
      val sqlText = "insert into bi_gamepublish_loginhour(login_time,game_id,parent_channel,child_channel,ad_label,game_account) values(?,?,?,?,?,?) " +
        "on duplicate key update game_account=?"
      val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        val channelArray = StringUtils.getArrayChannel(insertedRow.get(2).toString)
        if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {
          params.+=(Array[Any](insertedRow(0), insertedRow(1), channelArray(0), channelArray(1), channelArray(2), insertedRow(3), insertedRow(3)))
        }
      }
      try {
        MySqlUtils.doBatch(sqlText, params, conn)
      } finally {
        statement.close()
        conn.close
      }
    })
  }

  def foreachactivePartition(activedf: DataFrame) = {
    activedf.foreachPartition(rows => {
      val conn = MySqlUtils.getConn()
      val statement = conn.createStatement

      val sqlText = "insert into bi_game_active(game_id,parent_channel,child_channel,ad_label,imie,update_time) values(?,?,?,?,?,?)"
      val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        val channelArray = StringUtils.getArrayChannel(insertedRow.get(1).toString)
        if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {
          params.+=(Array[Any](insertedRow.get(0), channelArray(0), channelArray(1), channelArray(2), insertedRow.get(2), insertedRow.get(3)))
        }
      }
      try {
        MySqlUtils.doBatch(sqlText, params, conn)
      } finally {
        statement.close()
        conn.close
      }
    })
  }


}
