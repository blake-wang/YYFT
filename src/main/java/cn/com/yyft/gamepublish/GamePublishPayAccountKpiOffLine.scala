package cn.com.yyft.gamepublish

import cn.com.yyft.utils.{DateUtils, StringUtils, MySqlUtils, PropertiesUtils}
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by JSJSB-0071 on 2017/1/9.
  * 广告数据补充
  */
object GamePublishPayAccountKpiOffLine {
  val logger = Logger.getLogger(GamePublishPayAccountKpiOffLine.getClass)

  def main(args: Array[String]): Unit = {


    //跑数日期
    val startdday = args(0)
    val currentday = args(1)

    //    val conn = MySqlUtils.getConn()
    //    val statement = conn.createStatement
    //    //旧平台
    //    var mysqlText = "update bi_gamepublic_basekpi set pay_account_num=0,dau_pay_account_num=0 " +
    //      "where publish_time>='"+startdday +"' and publish_time <='"+currentday+" 59:59:59'"
    //    statement.executeUpdate(mysqlText);
    //    //新表
    //    mysqlText = "update bi_gamepublic_base_day_kpi set pay_account_num=0  " +
    //      "where publish_date>='"+startdday +"' and publish_date <='"+currentday+"'"
    //    statement.executeUpdate(mysqlText);
    //
    //    println("mysqlText: "+mysqlText)
    //    statement.close()
    //    conn.close()

    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.sql.shuffle.partitions", PropertiesUtils.getRelativePathValue("spark_sql_shuffle_partitions"))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use yyft")

    //bi_gamepublic_basekpi 充值账号数  pay_account_num,dau_pay_account_num
    val payAccountSql = "select split(tt.publish_time,':')[0],tt.game_id,tt.expand_channel,count(distinct tt.game_account) total \nfrom (\nselect distinct min(o.order_time) publish_time,o.game_id, if(r.expand_channel = '','21',r.expand_channel) expand_channel,lower(trim(o.game_account)) game_account \nfrom ods_regi_rz r \njoin ods_order o on lower(trim(o.game_account))=lower(trim(r.game_account))\njoin (select distinct game_id from ods_publish_game) pg on r.game_id=pg.game_id\nwhere o.game_id is not null and o.order_status=4 and to_date(o.order_time) >='startdday' and to_date(o.order_time) <='currentday'   \ngroup by to_date(o.order_time),o.game_id, if(r.expand_channel = '','21',r.expand_channel),lower(trim(o.game_account))\n) tt\ngroup by split(tt.publish_time,':')[0],tt.game_id,tt.expand_channel"
      .replace("startdday", startdday).replace("currentday", currentday)
    val payAccountDf = sqlContext.sql(payAccountSql)
    foreachPayAccountPartition(payAccountDf)


    //bi_gamepublic_base_day_kpi 充值账号数 pay_account_num
    //val payDayAccountSql="select split(tt.publish_time,' ')[0],tt.game_id,tt.expand_channel,count(distinct tt.game_account) total from (\nselect distinct min(o.order_time) publish_time,o.game_id, if(r.expand_channel = '','21',r.expand_channel) expand_channel,lower(trim(o.game_account)) game_account \nfrom ods_regi_rz r \njoin ods_order o on lower(trim(o.game_account))=lower(trim(r.game_account))\njoin (select distinct game_id from ods_publish_game) pg on r.game_id=pg.game_id\nwhere o.game_id is not null and o.order_status=4 and to_date(o.order_time) >='startdday' and to_date(o.order_time) <='currentday'  \ngroup by to_date(o.order_time),o.game_id, if(r.expand_channel = '','21',r.expand_channel),lower(trim(o.game_account))\n) tt\ngroup by split(tt.publish_time,' ')[0],tt.game_id,tt.expand_channel"
    val payDayAccountSql = "select split(tt.publish_time,' ')[0],tt.game_id,tt.expand_channel,count(distinct tt.game_account) total,tt.os,tt.group_id from (\nselect distinct o.order_time publish_time,o.game_id, if(r.expand_channel = '','21',r.expand_channel) expand_channel,lower(trim(o.game_account)) game_account,pg.os,pg.group_id\nfrom ods_regi_rz r \njoin ods_order o on lower(trim(o.game_account))=lower(trim(r.game_account))\njoin (select distinct old_game_id game_id,system_type os,group_id from game_sdk) pg on r.game_id=pg.game_id\nwhere o.game_id is not null and o.order_status=4 and prod_type=6 and to_date(o.order_time) >='startdday' and to_date(o.order_time) <='currentday'  \n) tt\ngroup by split(tt.publish_time,' ')[0],tt.game_id,tt.expand_channel,tt.os,tt.group_id "
      .replace("startdday", startdday).replace("currentday", currentday)
    val payDayAccountDf = sqlContext.sql(payDayAccountSql)
    foreachDayPayAccountPartition(payDayAccountDf)


    //保存点击
    saveClick(sqlContext, startdday, currentday)

    saveRequest(sc, sqlContext, startdday, currentday)

    System.clearProperty("spark.driver.port")
    sc.stop()
  }

  def saveRequest(sc: SparkContext, sqlContext: HiveContext, startdday: String, currentday: String): Unit = {
    //click off lime
    val requestRdd = sc.newAPIHadoopFile("hdfs://hadoopmaster:9000/user/hive/warehouse/yyft.db/request/*",
      classOf[CombineTextInputFormat],
      classOf[LongWritable],
      classOf[Text]).map(line => line._2.toString)
    //100.97.15.217 - - [29/Jun/2017:16:54:53 +0800] "GET /Ssxy/loadComplete?p=auc_fttst_3995M5017&g=3995&os_info=6.0&os_type=1 HTTP/1.0" 200 43 "https://tg.pyw.cn/h5/lhzy/index23.html?p=auc_fttst_3995M5017&g=3995""Mozilla/5.0 (Linux; U; Android 6.0; zh-CN; Le X620 Build/HEXCNFN5902606111S) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/40.0.2214.89 UCBrowser/11.5.6.946 Mobile Safari/537.36" 61.158.146.228, 106.39.190.139, 116.211.165.18 tg.pyw.cn magpie 0.006 0.007 -
    val filterRequestRdd = requestRdd.filter(line => !line.contains("HTTP/1.0\" 404")).filter(line => StringUtils.isRequestLog(line, ".*0800] \"GET /Ssxy/loadComplete[?]p=[\\d|_|a-z|A-Z]+&g=[\\d]+.*")).map(line => {
      try {
        val ip = line.split(" - - \\[", -1)(0)

        val sp = line.split(" ")(6).split("p=", -1)(1).split("\"\"", -1)(0)
        //expand_channel
        val sp1 = sp.split("&g=", -1)

        val requestDate = DateUtils.getDateForRequest(line)
        if (sp1(1) == "") {
          Row("pyw", 0, requestDate)
        } else {
          Row(StringUtils.defaultEmptyTo21(sp1(0)).toUpperCase(), Integer.parseInt(sp1(1).split("&", -1)(0)), requestDate + ":01:10")
        }
      } catch {
        case ex: Exception => {
          Row("pyw", 0, "0000-00-00 00:01:10")
        }
      }
    })
    val requestStruct = (new StructType).add("expand_channel", StringType)
      .add("game_id", IntegerType).add("publish_time", StringType)
    val requestDF = sqlContext.createDataFrame(filterRequestRdd, requestStruct)
    requestDF.registerTempTable("tmp_request")
    sqlContext.sql("select a.expand_channel,a.game_id,a.publish_time from tmp_request a join (select distinct game_id from ods_publish_game) b on a.game_id=b.game_id " +
      "where to_date(publish_time)>='" + startdday + "' and to_date(publish_time)<='" + currentday + "'").registerTempTable("tmp_filter_request")
    sqlContext.cacheTable("tmp_filter_request")
    //bi_gamepublic_base_day_kpi request_click_num
    val rquestDayDf = sqlContext.sql("select split(publish_time,' ')[0] publish_time,game_id,expand_channel,count(1) from tmp_filter_request " +
      "group by split(publish_time,' ')[0],game_id,expand_channel")
    requestDayForeachPartition(rquestDayDf)

    //bi_gamepublic_basekpi request_num
    val rquestHourDf = sqlContext.sql("select split(publish_time,':')[0] publish_time,game_id,expand_channel,count(1) from tmp_filter_request " +
      "group by split(publish_time,':')[0],game_id,expand_channel")
    requestHourForeachPartition(rquestHourDf)

    sqlContext.uncacheTable("tmp_filter_request")
  }

  def requestHourForeachPartition(clickHourDf: DataFrame): Unit = {
    clickHourDf.foreachPartition(rows => {
      val conn = MySqlUtils.getConn()
      val statement = conn.createStatement
      val sqlDayText = "insert into bi_gamepublic_basekpi(publish_time,game_id,parent_channel,child_channel,ad_label,request_num) " +
        "values(?,?,?,?,?,?) " +
        "on duplicate key update request_num=?"
      val paramsDay = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        //1：展示数日志，2：下载数日志;3:广告展示
        val channelArray = StringUtils.getArrayChannel(insertedRow.getString(2))
        if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {
          paramsDay.+=(Array[Any](insertedRow.getString(0), insertedRow.get(1), channelArray(0), channelArray(1), channelArray(2),
            insertedRow.get(3),
            insertedRow.get(3)))
        } else {
          logger.error("推广渠道格式错误：" + insertedRow.get(2) + " - " + insertedRow.get(1))
        }
      }
      try {
        MySqlUtils.doBatch(sqlDayText, paramsDay, conn)
      } finally {
        statement.close()
        conn.close
      }
    })
  }

  def requestDayForeachPartition(clickHourDf: DataFrame): Unit = {
    clickHourDf.foreachPartition(rows => {
      val conn = MySqlUtils.getConn()
      val statement = conn.createStatement
      val sqlDayText = "insert into bi_gamepublic_base_day_kpi(publish_date,child_game_id,medium_channel,ad_site_channel,pkg_code,request_click_num) " +
        "values(?,?,?,?,?,?) " +
        "on duplicate key update request_click_num=?"
      val paramsDay = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        //1：展示数日志，2：下载数日志;3:广告展示
        val channelArray = StringUtils.getArrayChannel(insertedRow.getString(2))
        if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {
          paramsDay.+=(Array[Any](insertedRow.getString(0), insertedRow.get(1), channelArray(0), channelArray(1), channelArray(2),
            insertedRow.get(3),
            insertedRow.get(3)))
        } else {
          logger.error("推广渠道格式错误：" + insertedRow.get(2) + " - " + insertedRow.get(1))
        }
      }
      try {
        MySqlUtils.doBatch(sqlDayText, paramsDay, conn)
      } finally {
        statement.close()
        conn.close
      }
    })
  }

  def saveClick(sqlContext: HiveContext, startdday: String, currentday: String): Unit = {

    sqlContext.sql("select upper(a.expand_channel) expand_channel,a.game_id,a.channel_time publish_time,a.channel_type from ods_channel a join (select distinct game_id from ods_publish_game) b on a.game_id=b.game_id " +
      "where  to_date(channel_time)>='" + startdday + "' and to_date(channel_time)<='" + currentday + "'").registerTempTable("filter_click_tmp")
    sqlContext.cacheTable("filter_click_tmp")

    //bi_gamepublic_basekpi  show_num
    val clickShowHourDf = sqlContext.sql("select split(publish_time,':')[0] publish_time,game_id,if(expand_channel = '','21',expand_channel) expand_channel,count(1) from filter_click_tmp " +
      "where channel_type = 1 group by split(publish_time,':')[0],game_id,if(expand_channel = '','21',expand_channel)")
    clickShowHourForeachPartition(clickShowHourDf)

    //bi_gamepublic_base_day_kpi  show_num
    val clickShowDayDf = sqlContext.sql("select split(publish_time,' ')[0] publish_time,game_id,if(expand_channel = '','21',expand_channel) expand_channel,count(1) from filter_click_tmp " +
      "where channel_type = 1 group by split(publish_time,' ')[0],game_id,if(expand_channel = '','21',expand_channel)")
    clickShowDayForeachPartition(clickShowDayDf)

    //bi_gamepublic_basekpi download_num
    val clickDownloadHourDf = sqlContext.sql("select split(publish_time,':')[0] publish_time,game_id,if(expand_channel = '','21',expand_channel) expand_channel,count(1) from filter_click_tmp " +
      "where channel_type = 2 group by split(publish_time,':')[0],game_id,if(expand_channel = '','21',expand_channel)")
    clickDownloadHourForeachPartition(clickDownloadHourDf)

    //bi_gamepublic_base_day_kpi download_num
    val clickDownloadDayDf = sqlContext.sql("select split(publish_time,' ')[0] publish_time,game_id,if(expand_channel = '','21',expand_channel) expand_channel,count(1) from filter_click_tmp " +
      "where channel_type = 2 group by split(publish_time,' ')[0],game_id,if(expand_channel = '','21',expand_channel)")
    clickDownloadDayForeachPartition(clickDownloadDayDf)

    //bi_gamepublic_basekpi adpage_click_num
    val clickAdHourDf = sqlContext.sql("select split(publish_time,':')[0] publish_time,game_id,if(expand_channel = '','21',expand_channel) expand_channel,count(1) from filter_click_tmp " +
      "where channel_type = 3 group by split(publish_time,':')[0],game_id,if(expand_channel = '','21',expand_channel)")
    clickAdHourForeachPartition(clickAdHourDf)

    //bi_gamepublic_basekpi adpage_click_num
    val clickAdDayDf = sqlContext.sql("select split(publish_time,' ')[0] publish_time,game_id,if(expand_channel = '','21',expand_channel) expand_channel,count(1) from filter_click_tmp " +
      "where channel_type = 3 group by split(publish_time,' ')[0],game_id,if(expand_channel = '','21',expand_channel)")
    clickAdDayForeachPartition(clickAdDayDf)

    sqlContext.uncacheTable("filter_click_tmp")
  }

  def clickShowDayForeachPartition(clickHourDf: DataFrame): Unit = {
    clickHourDf.foreachPartition(rows => {
      val conn = MySqlUtils.getConn()
      val sqlDayText = "insert into bi_gamepublic_base_day_kpi(publish_date,child_game_id,medium_channel,ad_site_channel,pkg_code,show_num) " +
        "values(?,?,?,?,?,?) " +
        "on duplicate key update show_num=?"
      // val sql = "update bi_gamepublic_base_day_kpi set show_num=? where publish_date=? and child_game_id=? and medium_channel=? and ad_site_channel=? and pkg_code=?"

      val paramsDay = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        //1：展示数日志，2：下载数日志;3:广告展示

        val channelArray = StringUtils.getArrayChannel(insertedRow.getString(2))

        if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {

          paramsDay.+=(Array[Any](insertedRow.getString(0), insertedRow.get(1), channelArray(0), channelArray(1), channelArray(2),
            insertedRow.get(3),
            insertedRow.get(3)))
        } else {
          logger.error("推广渠道格式错误：" + insertedRow.get(2) + " - " + insertedRow.get(1))
        }
      }

      MySqlUtils.doBatch(sqlDayText, paramsDay, conn)

      conn.close

    })
  }

  def clickShowHourForeachPartition(clickHourDf: DataFrame): Unit = {
    clickHourDf.foreachPartition(rows => {
      val conn = MySqlUtils.getConn()

      val sqlDayText = "insert into bi_gamepublic_basekpi(publish_time,game_id,parent_channel,child_channel,ad_label,show_num) " +
        "values(?,?,?,?,?,?) " +
        "on duplicate key update show_num=?"
      val paramsDay = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        //1：展示数日志，2：下载数日志;3:广告展示

        val channelArray = StringUtils.getArrayChannel(insertedRow.getString(2))
        if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {
          if (insertedRow.getString(2).endsWith("4283M10001") && insertedRow.getInt(1) == 4283) {
            println(insertedRow.getString(0) + " : " + insertedRow.getString(2) + " :hour: " + insertedRow.getInt(1) + " : " + insertedRow.get(3))
          }
          paramsDay.+=(Array[Any](insertedRow.getString(0), insertedRow.get(1), channelArray(0), channelArray(1), channelArray(2),
            insertedRow.get(3),
            insertedRow.get(3)))
          /*val ps = conn.prepareStatement(sqlDayText)
          ps.setString(1,insertedRow.getString(0))
          ps.setInt(2,insertedRow.getInt(1))
          ps.setString(3,channelArray(0))
          ps.setString(4,channelArray(1))
          ps.setString(5,channelArray(2))
          ps.setLong(6,insertedRow.getLong(3))
          ps.setLong(7,insertedRow.getLong(3))
          ps.executeUpdate()
          ps.close()*/
        } else {
          logger.error("推广渠道格式错误：" + insertedRow.get(2) + " - " + insertedRow.get(1))
        }
      }

      MySqlUtils.doBatch(sqlDayText, paramsDay, conn)


      conn.close

    })
  }

  def clickDownloadDayForeachPartition(clickHourDf: DataFrame): Unit = {
    clickHourDf.foreachPartition(rows => {
      val conn = MySqlUtils.getConn()
      val statement = conn.createStatement
      val sqlDayText = "insert into bi_gamepublic_base_day_kpi(publish_date,child_game_id,medium_channel,ad_site_channel,pkg_code,download_num) " +
        "values(?,?,?,?,?,?) " +
        "on duplicate key update download_num=?"
      val paramsDay = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        //1：展示数日志，2：下载数日志;3:广告展示
        val channelArray = StringUtils.getArrayChannel(insertedRow.getString(2))
        if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {
          paramsDay.+=(Array[Any](insertedRow.getString(0), insertedRow.get(1), channelArray(0), channelArray(1), channelArray(2),
            insertedRow.get(3),
            insertedRow.get(3)))
        } else {
          logger.error("推广渠道格式错误：" + insertedRow.get(2) + " - " + insertedRow.get(1))
        }
      }
      try {
        MySqlUtils.doBatch(sqlDayText, paramsDay, conn)
      } finally {
        statement.close()
        conn.close
      }
    })
  }

  def clickDownloadHourForeachPartition(clickHourDf: DataFrame): Unit = {
    clickHourDf.foreachPartition(rows => {
      val conn = MySqlUtils.getConn()
      val statement = conn.createStatement
      val sqlDayText = "insert into bi_gamepublic_basekpi(publish_time,game_id,parent_channel,child_channel,ad_label,download_num) " +
        "values(?,?,?,?,?,?) " +
        "on duplicate key update download_num=?"
      val paramsDay = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        //1：展示数日志，2：下载数日志;3:广告展示
        val channelArray = StringUtils.getArrayChannel(insertedRow.getString(2))
        if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {
          paramsDay.+=(Array[Any](insertedRow.getString(0), insertedRow.get(1), channelArray(0), channelArray(1), channelArray(2),
            insertedRow.get(3),
            insertedRow.get(3)))
        } else {
          logger.error("推广渠道格式错误：" + insertedRow.get(2) + " - " + insertedRow.get(1))
        }
      }
      try {
        MySqlUtils.doBatch(sqlDayText, paramsDay, conn)
      } finally {
        statement.close()
        conn.close
      }
    })
  }

  def clickAdDayForeachPartition(clickHourDf: DataFrame): Unit = {
    clickHourDf.foreachPartition(rows => {
      val conn = MySqlUtils.getConn()
      val statement = conn.createStatement
      val sqlDayText = "insert into bi_gamepublic_base_day_kpi(publish_date,child_game_id,medium_channel,ad_site_channel,pkg_code,adpage_click_num) " +
        "values(?,?,?,?,?,?) " +
        "on duplicate key update adpage_click_num=?"
      val paramsDay = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        //1：展示数日志，2：下载数日志;3:广告展示
        val channelArray = StringUtils.getArrayChannel(insertedRow.getString(2))
        if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {
          paramsDay.+=(Array[Any](insertedRow.getString(0), insertedRow.get(1), channelArray(0), channelArray(1), channelArray(2),
            insertedRow.get(3),
            insertedRow.get(3)))
        } else {
          logger.error("推广渠道格式错误：" + insertedRow.get(2) + " - " + insertedRow.get(1))
        }
      }
      try {
        MySqlUtils.doBatch(sqlDayText, paramsDay, conn)
      } finally {
        statement.close()
        conn.close
      }
    })
  }

  def clickAdHourForeachPartition(clickHourDf: DataFrame): Unit = {
    clickHourDf.foreachPartition(rows => {
      val conn = MySqlUtils.getConn()
      val statement = conn.createStatement
      val sqlDayText = "insert into bi_gamepublic_basekpi(publish_time,game_id,parent_channel,child_channel,ad_label,adpage_click_num) " +
        "values(?,?,?,?,?,?) " +
        "on duplicate key update adpage_click_num=?"
      val paramsDay = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        //1：展示数日志，2：下载数日志;3:广告展示
        val channelArray = StringUtils.getArrayChannel(insertedRow.getString(2))
        if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {
          paramsDay.+=(Array[Any](insertedRow.getString(0), insertedRow.get(1), channelArray(0), channelArray(1), channelArray(2),
            insertedRow.get(3),
            insertedRow.get(3)))
        } else {
          logger.error("推广渠道格式错误：" + insertedRow.get(2) + " - " + insertedRow.get(1))
        }
      }
      try {
        MySqlUtils.doBatch(sqlDayText, paramsDay, conn)
      } finally {
        statement.close()
        conn.close
      }
    })
  }

  def foreachDayPayAccountPartition(payAccountDf: DataFrame): Unit = {
    //把新增付费账号数结果存入mysql
    payAccountDf.foreachPartition(rows => {
      val conn = MySqlUtils.getConn()
      val statement = conn.createStatement

      val sqlDayText = "insert into bi_gamepublic_base_day_kpi(publish_date,child_game_id,medium_channel,ad_site_channel,pkg_code,pay_account_num,os,group_id) " +
        "values(?,?,?,?,?,?,?,?) " +
        "on duplicate key update pay_account_num=?,os=?,group_id=?"
      val paramsDay = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {

        if (insertedRow.get(2) != null) {
          val channelArray = StringUtils.getArrayChannel(insertedRow.get(2).toString)
          if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {

            paramsDay.+=(Array[Any](insertedRow.getString(0), insertedRow.get(1), channelArray(0), channelArray(1), channelArray(2), insertedRow.get(3), insertedRow.get(4), insertedRow.get(5),
              insertedRow.get(3), insertedRow.get(4), insertedRow.get(5)))
          } else {
            logger.error("推广渠道格式错误：" + insertedRow.get(2) + " - " + insertedRow.get(1))
          }
        } else {
          println("=======>" + insertedRow.get(2) + " - " + insertedRow.get(0) + " - " + insertedRow.get(1) + " - " + insertedRow.get(3))
        }
      }
      try {
        MySqlUtils.doBatch(sqlDayText, paramsDay, conn)
      } finally {
        statement.close()
        conn.close
      }
    })
  }

  //充值账号数 离线处理
  def foreachPayAccountPartition(payAccountDf: DataFrame): Unit = {
    payAccountDf.foreachPartition(rows => {
      val conn = MySqlUtils.getConn()

      val sqlText = "insert into bi_gamepublic_basekpi(publish_time,game_id,parent_channel,child_channel,ad_label,pay_account_num,dau_pay_account_num) " +
        "values(?,?,?,?,?,?,?) " +
        "on duplicate key update pay_account_num=?,dau_pay_account_num=?"

      val params = new ArrayBuffer[Array[Any]]()

      for (insertedRow <- rows) {

        if (insertedRow.get(2) != null) {
          val channelArray = StringUtils.getArrayChannel(insertedRow.get(2).toString)
          if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {

            params.+=(Array[Any](insertedRow.get(0), insertedRow.get(1), channelArray(0), channelArray(1), channelArray(2), insertedRow.get(3), insertedRow.get(3),
              insertedRow.get(3), insertedRow.get(3)))

          } else {
            logger.error("推广渠道格式错误：" + insertedRow.get(2) + " - " + insertedRow.get(1))
          }
        } else {
          println("=======>" + insertedRow.get(2) + " - " + insertedRow.get(0) + " - " + insertedRow.get(1) + " - " + insertedRow.get(3))
        }
      }
      try {
        MySqlUtils.doBatch(sqlText, params, conn)
      } finally {
        conn.close
      }
    })
  }
}
