package cn.com.yyft.mllib


import cn.com.yyft.utils.MySqlUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.types.{StringType, IntegerType, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
 * Created by sumenghu on 2016/12/7.
 */
object DownloadCF {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DownloadCF")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use yyft")
    hiveContext.sql("select distinct imei,game_id from ods_appdownload").registerTempTable("app_download")

    hiveContext.sql("select ad.game_id,ad1.game_id game_id_rs,count(ad1.game_id) download_num from app_download ad " +
      "join app_download ad1 on ad.imei=ad1.imei and ad.game_id <> ad1.game_id " +
      "group by ad.game_id,ad1.game_id order by ad.game_id").registerTempTable("download_result")

    hiveContext.sql("select game_id,CONCAT(download_num,'_',game_id_rs) top_game_id from download_result").registerTempTable("download_result_top")

    hiveContext.sql("select sub_table.game_id,sub_table.top_game_id from " +
      "(select game_id,top_game_id,row_number() over (partition by game_id order by top_game_id) as rn from download_result_top) sub_table " +
      "where sub_table.rn <=80 order by sub_table.game_id, sub_table.rn desc" ).registerTempTable("download_result_top80")

    val resultDf = hiveContext.sql("select game_id,concat_ws('|',collect_set(top_game_id)) from (select game_id,split(top_game_id,'_')[1] top_game_id from download_result_top80) t group by game_id")

    resultDf.foreachPartition(rows => {
      val conn = MySqlUtils.getConn()

      val sqlText = " insert into bi_app_game_recommend(game_id,game_id_rs)" +
        " values(?,?) on duplicate key update game_id_rs=?"
      val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        params.+=(Array[Any](insertedRow.get(0),insertedRow.get(1),insertedRow.get(1)))
      }
      try {
        MySqlUtils.doBatch(sqlText, params, conn)
      } finally {
        conn.close
      }
    })

    sc.stop()
  }

}
