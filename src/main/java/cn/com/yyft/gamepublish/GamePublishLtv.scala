package cn.com.yyft.gamepublish

import cn.com.yyft.utils.{MySqlUtils, PropertiesUtils, StringUtils}
import org.apache.log4j.Logger
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by 苏孟虎 on 2016/8/29.
  * 对游戏发布数据报表统计-生命周期
  * 次日LTV：（1月1日新增用户在1月1日至1月2日的充值总金额）/1月1日新增用户数
  * 三日LTV：（1月1日新增用户在1月1日至1月3日的充值总金额）/1月1日新增用户数
  */
object GamePublishLtv extends Logging {
  val logger = Logger.getLogger(GamePublishLtv.getClass)

  def main(args: Array[String]): Unit = {


    logger.error("start the ltv app ")
    val currentday = args(0)

    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.sql.shuffle.partitions", PropertiesUtils.getRelativePathValue("spark_sql_shuffle_partitions"))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use yyft")
    //val hivesql="select \n  retained_tmp.reg_time,\n  retained_tmp.game_id,\n  if(retained_tmp.channel_id = '','21',retained_tmp.channel_id) ,\n  reg_count.reg_sum add_user_count,\n  retained_tmp.retained_1day,\n  retained_tmp.retained_2day,\n  retained_tmp.retained_3day,\n  retained_tmp.retained_4day,\n  retained_tmp.retained_5day,\n  retained_tmp.retained_6day,\n  retained_tmp.retained_14day,\n  retained_tmp.retained_29day,\n  retained_tmp.retained_59day\n from(\nselect \n  rs.reg_time,\n  rs.game_id,\n  expand_channel channel_id,\n  sum(CASE rs.dur WHEN 1 THEN rs.amount ELSE 0 END ) as retained_1day,\n  sum(CASE rs.dur WHEN 2 THEN rs.amount ELSE 0 END ) as retained_2day,\n  sum(CASE rs.dur WHEN 3 THEN rs.amount ELSE 0 END ) as retained_3day,\n  sum(CASE rs.dur WHEN 4 THEN rs.amount ELSE 0 END ) as retained_4day,\n  sum(CASE rs.dur WHEN 5 THEN rs.amount ELSE 0 END ) as retained_5day,\n  sum(CASE rs.dur WHEN 6 THEN rs.amount ELSE 0 END ) as retained_6day,\n  sum(CASE rs.dur WHEN 14 THEN rs.amount ELSE 0 END ) as retained_14day,\n  sum(CASE rs.dur WHEN 29 THEN rs.amount ELSE 0 END ) as retained_29day,\n  sum(CASE rs.dur WHEN 59 THEN rs.amount ELSE 0 END ) as retained_59day\nFROM\n(\n select filterOdsR.reg_time,filterOdsR.game_id,if(filterOdsR.expand_channel = '','21',filterOdsR.expand_channel) expand_channel, \n datediff(odsl.order_time,filterOdsR.reg_time) dur,filterOdsR.game_account,if(odsl.order_status = 4,odsl.ori_price,-odsl.ori_price) amount\n   from (\n      select to_date(odsr.reg_time) reg_time,odsr.game_id,odsr.expand_channel,odsr.game_account from ods_regi_rz odsr \n         where to_date(odsr.reg_time)<='currentday' and to_date (odsr.reg_time) >= date_add(\"currentday\",-59)\n    )filterOdsR\n      join ods_order odsl on filterOdsR.game_account = odsl.game_account\n    where to_date(odsl.order_time) > filterOdsR.reg_time and datediff(odsl.order_time,filterOdsR.reg_time) in(1,2,3,4,5,6,14,29,59) \n           and odsl.order_status in(4,8)\n) rs \ngroup BY rs.reg_time,rs.game_id,rs.expand_channel\n) retained_tmp\n  join (select to_date(odsr.reg_time) reg_time,odsr.game_id,if(odsr.expand_channel = '','21',odsr.expand_channel) expand_channel,count(odsr.game_account) reg_sum from ods_regi_rz odsr \n         where to_date(odsr.reg_time)<='currentday' and to_date (odsr.reg_time) >= date_add(\"currentday\",-59)\n         group by to_date(odsr.reg_time),game_id,if(odsr.expand_channel = '','21',odsr.expand_channel)) reg_count\n  on retained_tmp.reg_time = reg_count.reg_time and retained_tmp.game_id = reg_count.game_id and retained_tmp.channel_id = reg_count.expand_channel\n  join (select distinct game_id from ods_publish_game) pg on retained_tmp.game_id = pg.game_id"

    //过滤出符合条件有用账号: 1,关联深度联运。2,时间范围当前日往前60天
    val filterRegiSql = "select to_date(reg.reg_time) reg_time,reg.game_id game_id," +
      "if(reg.expand_channel = '','21',reg.expand_channel) expand_channel,reg.game_account game_account from ods_regi_rz reg " +
      "join (select distinct game_id from ods_publish_game) pg on reg.game_id = pg.game_id " +
      "where to_date(reg.reg_time)<='currentday' and to_date (reg.reg_time) >= date_add('currentday',-59)"
        .replace("currentday", currentday)
    val filterRegiDF = sqlContext.sql(filterRegiSql)
    filterRegiDF.cache()
    filterRegiDF.registerTempTable("filter_regi")

    /**
      * 业务定义：
      * 次日LTV：（1月1日新增用户在1月1日至1月2日的充值总金额）/1月1日新增用户数
      * 三日LTV：（1月1日新增用户在1月1日至1月3日的充值总金额）/1月1日新增用户数
      */
    val ltvDayArray = Array[Int](2, 3, 4, 5, 6, 7, 15, 30, 60)
    for (ltvDay <- ltvDayArray) {
      val ltvDaySql = "select reg.reg_time,reg.game_id,reg.expand_channel,sum(if(o.order_status = 4,o.ori_price,-o.ori_price)) amount " +
        "from filter_regi reg join ods_order o on reg.game_account = o.game_account and o.order_status in(4,8) " +
        "where to_date(o.order_time)>=reg.reg_time and to_date(o.order_time)<date_add(reg.reg_time," + ltvDay + ") " +
        "group by reg.reg_time,reg.game_id,reg.expand_channel"
      sqlContext.sql(ltvDaySql).registerTempTable("ltv" + ltvDay + "_day")
    }


    //获取游戏id，推广渠道，对应的注册账号数
    val accountCountSql = "select reg.reg_time,reg.game_id,reg.expand_channel,count(reg.game_account) account_count " +
      "from filter_regi reg group by reg.reg_time,reg.game_id,reg.expand_channel"
    sqlContext.sql(accountCountSql).registerTempTable("account_count")

    val resultDf = sqlContext.sql("select account.reg_time,account.game_id,account.expand_channel,account.account_count," +
      "if(ltv2day.amount is null,0,ltv2day.amount) ltv2_amount,if(ltv3day.amount is null,0,ltv3day.amount) ltv3_amount," +
      "if(ltv4day.amount is null,0,ltv4day.amount) ltv4_amount,if(ltv5day.amount is null,0,ltv5day.amount) ltv5_amount" +
      ",if(ltv6day.amount is null,0,ltv6day.amount) ltv6_amount,if(ltv7day.amount is null,0,ltv7day.amount) ltv7_amount," +
      "if(ltv15day.amount is null,0,ltv15day.amount) ltv15_amount,if(ltv30day.amount is null,0,ltv30day.amount) ltv30_amount," +
      "if(ltv60day.amount is null,0,ltv60day.amount) ltv60_amount " +
      "from account_count account " +
      "left join ltv2_day ltv2day on ltv2day.reg_time=account.reg_time and ltv2day.game_id=account.game_id and ltv2day.expand_channel=account.expand_channel " +
      "left join ltv3_day ltv3day on account.reg_time=ltv3day.reg_time and account.game_id=ltv3day.game_id and account.expand_channel=ltv3day.expand_channel " +
      "left join ltv4_day ltv4day on ltv4day.reg_time=account.reg_time and ltv4day.game_id=account.game_id and ltv4day.expand_channel=account.expand_channel " +
      "left join ltv5_day ltv5day on account.reg_time=ltv5day.reg_time and account.game_id=ltv5day.game_id and account.expand_channel=ltv5day.expand_channel " +
      "left join ltv6_day ltv6day on ltv6day.reg_time=account.reg_time and ltv6day.game_id=account.game_id and ltv6day.expand_channel=account.expand_channel " +
      "left join ltv7_day ltv7day on account.reg_time=ltv7day.reg_time and account.game_id=ltv7day.game_id and account.expand_channel=ltv7day.expand_channel " +
      "left join ltv15_day ltv15day on ltv15day.reg_time=account.reg_time and ltv15day.game_id=account.game_id and ltv15day.expand_channel=account.expand_channel " +
      "left join ltv30_day ltv30day on account.reg_time=ltv30day.reg_time and account.game_id=ltv30day.game_id and account.expand_channel=ltv30day.expand_channel " +
      "left join ltv60_day ltv60day on ltv60day.reg_time=account.reg_time and ltv60day.game_id=account.game_id and ltv60day.expand_channel=account.expand_channel")


    //把结果存入mysql
    resultDf.foreachPartition(rows => {
      val conn = MySqlUtils.getConn()
      val statement = conn.createStatement

      val sqlText = " insert into bi_gamepublic_retainedltv(reg_time,game_id,parent_channel,child_channel,ad_label,add_user_num," +
        "ltv_1day,ltv_2day,ltv_3day,ltv_4day,ltv_5day,ltv_6day,ltv_14day,ltv_29day,ltv_59day)" +
        " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" +
        " on duplicate key update add_user_num=?,ltv_1day=?,ltv_2day=?,ltv_3day=?,ltv_4day=?,ltv_5day=?,ltv_6day=?,ltv_14day=?,ltv_29day=?,ltv_59day=?"
      val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        if (insertedRow.get(2) != null) {
          val channelArray = StringUtils.getArrayChannel(insertedRow.get(2).toString)
          if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {
            params.+=(Array[Any](insertedRow.get(0), insertedRow.get(1), channelArray(0), channelArray(1), channelArray(2),
              insertedRow.get(3), insertedRow.get(4), insertedRow.get(5), insertedRow.get(6)
              , insertedRow.get(7), insertedRow.get(8), insertedRow.get(9), insertedRow.get(10), insertedRow.get(11), insertedRow.get(12),
              insertedRow.get(3), insertedRow.get(4), insertedRow.get(5), insertedRow.get(6)
              , insertedRow.get(7), insertedRow.get(8), insertedRow.get(9), insertedRow.get(10), insertedRow.get(11), insertedRow.get(12)
            ))
          } else {
            logger.error("expand_channel format error: " + insertedRow.get(2) + " - " + insertedRow.get(1))
          }
        } else {
          logger.error("expand_channel is null: " + insertedRow.get(0) + " - " + insertedRow.get(2) + " - " + insertedRow.get(1))
        }
      }
      try {
        MySqlUtils.doBatch(sqlText, params, conn)
      } finally {
        statement.close()
        conn.close
      }
    })
    System.clearProperty("spark.driver.port")
    sc.stop()
  }
}
