package cn.com.yyft.gamepublish

import cn.com.yyft.utils.{MySqlUtils, PropertiesUtils, StringUtils}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by JSJSB-0071 on 2016/12/27.
  * bi_gamepublic_basekpi:pay_money  pay_money_new regi_account_num
  */
object GamePublishBaseKpiOffLine {

  val logger = Logger.getLogger(GamePublishBaseKpiOffLine.getClass)


  def main(args: Array[String]): Unit = {


    //跑数日期
    val startdday = args(0)
    val currentday = args(1)

    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.sql.shuffle.partitions", PropertiesUtils.getRelativePathValue("spark_sql_shuffle_partitions")).setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use yyft")

    //bi_gamepublic_basekpi  pay_money(到小时）


    val orderAmountSql = "select split(o.order_time,':')[0] publish_time,o.game_id,r.expand_channel,sum(o.ori_price) pay_money from \n        (select distinct * from ods_order where order_status =4 and to_date(order_time)>='startdday' and to_date(order_time) <='currentday') o \n        join (select distinct rz.game_account,if(rz.expand_channel = '','21',rz.expand_channel) expand_channel\n                    from ods_regi_rz rz join (select distinct game_id from ods_publish_game) pg on rz.game_id=pg.game_id) r \n        on lower(trim(o.game_account))=lower(trim(r.game_account))\n        group by r.expand_channel,o.game_id,split(o.order_time,':')[0]"
      .replace("startdday", startdday).replace("currentday", currentday)
    val orderAmountDf = sqlContext.sql(orderAmountSql)
    foreachOrderAmountPartition(orderAmountDf)

    //bi_gamepublic_basekpi  pay_money_new (到小时）
    val orderNewAmountSql = "select split(o.order_time,':')[0] publish_time,o.game_id,r.expand_channel,sum(o.ori_price+o.total_amt) pay_money from \n        (select distinct * from ods_order where order_status =4 and prod_type=6 and to_date(order_time)>='startdday' and to_date(order_time) <='currentday') o \n        join (select distinct rz.game_account,if(rz.expand_channel = '','21',rz.expand_channel) expand_channel\n                    from ods_regi_rz rz join (select distinct game_id from ods_publish_game) pg on rz.game_id=pg.game_id) r \n        on lower(trim(o.game_account))=lower(trim(r.game_account))\n        group by r.expand_channel,o.game_id,split(o.order_time,':')[0]"
      .replace("startdday", startdday).replace("currentday", currentday)
    val orderNewAmountDf = sqlContext.sql(orderNewAmountSql)
    foreachNewOrderAmountPartition(orderNewAmountDf)

    //新平台bi_gamepublic_base_day_kpi充值流水 pay_money（到天）
    //val orderNewDayAmountSql = "select split(o.order_time,' ')[0] publish_time,o.game_id,r.expand_channel,sum(o.ori_price+o.total_amt) pay_money from \n        (select distinct * from ods_order where order_status =4 and prod_type=6 and to_date(order_time)>='startdday' and to_date(order_time) <='currentday') o \n        join (select distinct rz.game_account,if(rz.expand_channel = '','21',rz.expand_channel) expand_channel\n                    from ods_regi_rz rz join (select distinct game_id from ods_publish_game) pg on rz.game_id=pg.game_id) r \n        on lower(trim(o.game_account))=lower(trim(r.game_account))\n        group by r.expand_channel,o.game_id,split(o.order_time,' ')[0]"
    val orderNewDayAmountSql = "select split(o.order_time,' ')[0] publish_time,o.game_id,r.expand_channel,sum(o.ori_price+o.total_amt) pay_money,r.os,r.group_id from \n(select distinct * from ods_order where order_status =4 and prod_type=6 and to_date(order_time)>='startdday' and to_date(order_time) <='currentday') o \njoin (select distinct rz.game_account,if(rz.expand_channel = '','21',rz.expand_channel) expand_channel,pg.os,pg.group_id from ods_regi_rz rz \njoin (select distinct old_game_id game_id,system_type os,group_id from game_sdk) pg on rz.game_id=pg.game_id\n) r \non lower(trim(o.game_account))=lower(trim(r.game_account))\ngroup by r.expand_channel,o.game_id,split(o.order_time,' ')[0],r.os,r.group_id "
      .replace("startdday", startdday).replace("currentday", currentday)
    val orderNewDayAmountDf = sqlContext.sql(orderNewDayAmountSql)
    foreachNewDayOrderAmountPartition(orderNewDayAmountDf)

    //bi_gamepublic_basekpi对应的注册账号数 regi_account_num (到小时）
    val accountCountSql = "select split(rz.reg_time,':')[0] publish_time,rz.game_id, if(rz.expand_channel = '','21',rz.expand_channel) expand_channel,count(distinct game_account) regi_num \nfrom ods_regi_rz rz join (select distinct game_id from ods_publish_game) pg on rz.game_id=pg.game_id \nwhere to_date(rz.reg_time) >='startdday' and  to_date(rz.reg_time) <='currentday' group by if(rz.expand_channel = '','21',rz.expand_channel),rz.game_id,split(rz.reg_time,':')[0]"
    val accountDf = sqlContext.sql(accountCountSql.replace("startdday", startdday).replace("currentday", currentday))
    foreachAccountPartition(accountDf)


    //bi_gamepublic_base_day_kpi对应的注册账号数 regi_account_num （到天）
    //val accountDayCountSql = "select split(rz.reg_time,' ')[0] publish_time,rz.game_id, if(rz.expand_channel = '','21',rz.expand_channel) expand_channel,count(distinct game_account) regi_num \nfrom ods_regi_rz rz join (select distinct game_id from ods_publish_game) pg on rz.game_id=pg.game_id \nwhere to_date(rz.reg_time) >='startdday' and  to_date(rz.reg_time) <='currentday' group by if(rz.expand_channel = '','21',rz.expand_channel),rz.game_id,split(rz.reg_time,' ')[0]"
    val accountDayCountSql = "select split(rz.reg_time,' ')[0] publish_time,rz.game_id, if(rz.expand_channel = '','21',rz.expand_channel) expand_channel,count(distinct game_account) regi_num ,pg.os,pg.group_id,parent_game_id,count(distinct rz.imei) regi_device_num\nfrom ods_regi_rz rz join (select distinct old_game_id game_id,system_type os,group_id,game_id as parent_game_id from game_sdk) pg on rz.game_id=pg.game_id \nwhere to_date(rz.reg_time) >='startdday' and  to_date(rz.reg_time) <='currentday' \ngroup by if(rz.expand_channel = '','21',rz.expand_channel),rz.game_id,split(rz.reg_time,' ')[0],pg.os,pg.group_id,parent_game_id"
    val accountDayDf = sqlContext.sql(accountDayCountSql.replace("startdday", startdday).replace("currentday", currentday))
    foreachDayAccountPartition(accountDayDf)

    //  bi_gamepublic_base_opera_kpi 注册设备数 regi_device_num （到天 游戏维度）
    val deviceDayGameCountSql = "select split(rz.reg_time,' ')[0] publish_time,rz.game_id,pg.os,pg.group_id,parent_game_id,count(distinct rz.imei) regi_device_num\nfrom ods_regi_rz rz join (select distinct old_game_id game_id,system_type os,group_id,game_id as parent_game_id from game_sdk) pg on rz.game_id=pg.game_id \nwhere to_date(rz.reg_time) >='startdday' and  to_date(rz.reg_time) <='currentday' \ngroup by rz.game_id,split(rz.reg_time,' ')[0],pg.os,pg.group_id,parent_game_id"
    val deviceDayGameDf = sqlContext.sql(deviceDayGameCountSql.replace("startdday", startdday).replace("currentday", currentday))
    foreachDayDeviceGamePartition(deviceDayGameDf)

    System.clearProperty("spark.driver.port")
    sc.stop()
  }

  def foreachDayAccountPartition(accountDf: DataFrame): Unit = {
    //把注册账号数结果存入mysql
    accountDf.foreachPartition(rows => {
      val conn = MySqlUtils.getConn()
      val statement = conn.createStatement

      val sqlDayText = "insert into bi_gamepublic_base_day_kpi(publish_date,child_game_id,medium_channel,ad_site_channel,pkg_code,regi_account_num,os,group_id,parent_game_id,regi_device_num) " +
        "values(?,?,?,?,?,?,?,?,?,?) " +
        "on duplicate key update regi_account_num=?,os=?,group_id=?,parent_game_id=?,regi_device_num=?"

      val paramsDay = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        if (insertedRow.get(2) != null) {
          val channelArray = StringUtils.getArrayChannel(insertedRow.get(2).toString)
          if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {

            paramsDay.+=(Array[Any](insertedRow.getString(0), insertedRow.get(1), channelArray(0), channelArray(1), channelArray(2), insertedRow.get(3), insertedRow.get(4), insertedRow.get(5), insertedRow.get(6), insertedRow.get(7),
              insertedRow.get(3), insertedRow.get(4), insertedRow.get(5), insertedRow.get(6), insertedRow.get(7)))
          } else {
            logger.error("推广渠道格式错误：" + insertedRow.get(2) + " - " + insertedRow.get(1))
          }
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

  def foreachAccountPartition(accountDf: DataFrame): Unit = {
    //把注册账号数结果存入mysql
    accountDf.foreachPartition(rows => {
      val conn = MySqlUtils.getConn()
      val statement = conn.createStatement

      val sqlText = "insert into bi_gamepublic_basekpi(publish_time,game_id,parent_channel,child_channel,ad_label,regi_account_num) " +
        "values(?,?,?,?,?,?) " +
        "on duplicate key update regi_account_num=?"

      val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        if (insertedRow.get(2) != null) {
          val channelArray = StringUtils.getArrayChannel(insertedRow.get(2).toString)
          if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {
            params.+=(Array[Any](insertedRow.get(0), insertedRow.get(1), channelArray(0), channelArray(1), channelArray(2), insertedRow.get(3),
              insertedRow.get(3)))
          } else {
            logger.error("推广渠道格式错误：" + insertedRow.get(2) + " - " + insertedRow.get(1))
          }
        }
      }
      try {
        MySqlUtils.doBatch(sqlText, params, conn)
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        statement.close()
        conn.close
      }
    })
  }

  def foreachOrderAmountPartition(orderAmountDf: DataFrame): Unit = {
    //把流水结果存入mysql
    orderAmountDf.foreachPartition(rows => {
      val conn = MySqlUtils.getConn()
      val statement = conn.createStatement

      val sqlText = "insert into bi_gamepublic_basekpi(publish_time,game_id,parent_channel,child_channel,ad_label,pay_money) " +
        "values(?,?,?,?,?,?) " +
        "on duplicate key update pay_money=?"
      val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        if (insertedRow.get(2) != null) {
          val channelArray = StringUtils.getArrayChannel(insertedRow.get(2).toString)
          if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {
            params.+=(Array[Any](insertedRow.get(0), insertedRow.get(1), channelArray(0), channelArray(1), channelArray(2), insertedRow.get(3),
              insertedRow.get(3)))
          } else {
            logger.error("推广渠道格式错误：" + insertedRow.get(2) + " - " + insertedRow.get(1))
          }
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

  def foreachNewOrderAmountPartition(orderAmountDf: DataFrame): Unit = {
    //把流水结果存入mysql
    orderAmountDf.foreachPartition(rows => {
      val conn = MySqlUtils.getConn()
      val statement = conn.createStatement

      val sqlText = "insert into bi_gamepublic_basekpi(publish_time,game_id,parent_channel,child_channel,ad_label,pay_money_new) " +
        "values(?,?,?,?,?,?) " +
        "on duplicate key update pay_money_new=?"
      val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        if (insertedRow.get(2) != null) {
          val channelArray = StringUtils.getArrayChannel(insertedRow.get(2).toString)
          if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {
            params.+=(Array[Any](insertedRow.get(0), insertedRow.get(1), channelArray(0), channelArray(1), channelArray(2), insertedRow.get(3),
              insertedRow.get(3)))
          } else {
            logger.error("推广渠道格式错误：" + insertedRow.get(2) + " - " + insertedRow.get(1))
          }
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

  def foreachNewDayOrderAmountPartition(orderAmountDf: DataFrame): Unit = {
    //把流水结果存入mysql
    orderAmountDf.foreachPartition(rows => {
      val conn = MySqlUtils.getConn()
      val statement = conn.createStatement

      val sqlText = "insert into bi_gamepublic_base_day_kpi(publish_date,child_game_id,medium_channel,ad_site_channel,pkg_code,pay_money,os,group_id) " +
        "values(?,?,?,?,?,?,?,?) " +
        "on duplicate key update pay_money=?,os=?,group_id=?"
      val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        if (insertedRow.get(2) != null) {
          val channelArray = StringUtils.getArrayChannel(insertedRow.get(2).toString)
          if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {
            params.+=(Array[Any](insertedRow.get(0), insertedRow.get(1), channelArray(0), channelArray(1), channelArray(2), insertedRow.get(3), insertedRow.get(4), insertedRow.get(5),
              insertedRow.get(3), insertedRow.get(4), insertedRow.get(5)))
          } else {
            logger.error("推广渠道格式错误：" + insertedRow.get(2) + " - " + insertedRow.get(1))
          }
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

  def foreachDayDeviceGamePartition(accountDayGameDf: DataFrame) = {
    //把注册账号数结果存入mysql
    accountDayGameDf.foreachPartition(rows => {
      val conn = MySqlUtils.getConn()
      val statement = conn.createStatement

      val sqlDayText = "insert into bi_gamepublic_base_opera_kpi(publish_date,child_game_id,os,group_id,parent_game_id,regi_device_num) " +
        "values(?,?,?,?,?,?) " +
        "on duplicate key update os=values(os),group_id=values(group_id),parent_game_id=values(parent_game_id),regi_device_num=values(regi_device_num)"

      val paramsDay = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        if (insertedRow.get(2) != null) {
          val channelArray = StringUtils.getArrayChannel(insertedRow.get(2).toString)

          paramsDay.+=(Array[Any](insertedRow.getString(0), insertedRow.get(1), insertedRow.get(2), insertedRow.get(3), insertedRow.get(4), insertedRow.get(5)))
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

}
