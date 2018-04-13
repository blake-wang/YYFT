package cn.com.yyft.sdk

import java.io.File

import cn.com.yyft.utils.MySqlUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
 * Sdk 对账表
 */
object SdkBillOps {
  def main(args: Array[String]): Unit = {

    //跑数日期
    val startdday = args(0)
    val currentday = args(1)
      //猛虎
    //val hivesql = "select to_date (od.req_time) month_date,od.game_id,if(regi.account_channel is null ,0,regi.account_channel) account_channel,\nif(regi.os is null,'',regi.os) os,sum(od.amount) amount,\nif(bg.corporation_name is null ,'',bg.corporation_name) corporation_name\nfrom pywsdk_cp_req od\nleft join bgameaccount regi on lower(trim(regi.account))=lower(trim(od.account))\nleft join bgame bg on od.game_id = bg.id\nwhere to_date(od.req_time) >='startdday' and  to_date(od.req_time) <'currentday'\n  and od.status=1\ngroup by \nto_date (od.req_time),od.game_id,regi.account_channel,regi.os,bg.corporation_name"
    //按月加载
    val hivesql="select to_date (od.req_time) month_date,od.game_id,if(regi.account_channel is null ,0,regi.account_channel) account_channel,\nif(regi.os is null,'',regi.os) os,sum(od.amount) amount,\nif(bg.corporation_name is null ,'',bg.corporation_name) corporation_name\nfrom pywsdk_cp_req od\nleft join bgameaccount regi on lower(trim(regi.account))=lower(trim(od.account))\nleft join bgame bg on od.game_id = bg.id\nleft join (select order_sn,sandbox from pywsdk_apple_receipt_verify ver where ver.sandbox=1 and state=3) ver on ver.order_sn=od.order_no \nwhere to_date(od.req_time) >='startdday' and  to_date(od.req_time) <'currentday'\n  and od.status=1 and ver.order_sn is null\ngroup by \nto_date (od.req_time),od.game_id,regi.account_channel,regi.os,bg.corporation_name"
    //加载历史 1609-1706
    //val hivesql="select to_date (od.req_time) month_date,od.game_id,if(regi.account_channel is null ,0,regi.account_channel) account_channel,\nif(regi.os is null,'',regi.os) os,sum(od.amount) amount,\nif(bg.corporation_name is null ,'',bg.corporation_name) corporation_name\nfrom pywsdk_cp_req od\nleft join tmp.bgacc1 regi on lower(trim(regi.account))=lower(trim(od.account))\nleft join bgame bg on od.game_id = bg.id\nleft join (select order_sn,sandbox from pywsdk_apple_receipt_verify ver where ver.sandbox=1 and state=3) ver on ver.order_sn=od.order_no \nwhere to_date(od.req_time) >='2016-09-01' and  to_date(od.req_time) <'2017-07-01'\n  and od.status=1 and ver.order_sn is null\ngroup by \nto_date (od.req_time),od.game_id,regi.account_channel,regi.os,bg.corporation_name"
    val execSql = hivesql.replace("startdday", startdday).replace("currentday", currentday) //hive sql

    /** ******************hive库操作 *******************/
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use yyft")
    val dataf = sqlContext.sql(execSql) //执行hive sql

    /** ******************数据库操作 *******************/
    dataf.foreachPartition(rows => {
      val conn = MySqlUtils.getConn()

      val sqlText = " insert into bi_sdk_bills(month_date,game_id,channel_id,os,amount,corporation_name)" +
        " values(?,?,?,?,?,?) on duplicate key update amount=?,corporation_name=?"
      val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        params.+=(Array[Any](insertedRow.get(0),insertedRow.get(1),insertedRow.get(2),
                      if(insertedRow.get(3)==null) "" else insertedRow.get(3),insertedRow.get(4),insertedRow.get(5),insertedRow.get(4),insertedRow.get(5)))
      }
      try {
        MySqlUtils.doBatch(sqlText, params, conn)
      } finally {
        conn.close
      }
    })

    System.clearProperty("spark.driver.port")
    sc.stop()
  }

  def setHadoopLibariy(): Unit = {
    //Hadoop libariy
    val path: String = new File(".").getCanonicalPath
    System.getProperties().put("hadoop.home.dir", path)
    new File("./bin").mkdirs()
    new File("./bin/winutils.exe").createNewFile()
  }

}