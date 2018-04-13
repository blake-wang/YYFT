package cn.com.yyft.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, DoubleType, StringType, StructType}
import org.apache.spark.sql.{Row, SQLContext}

/**
 * Created by JSJSB-0071 on 2016/10/31.
 */
import redis.clients.jedis.Jedis

object AppStreamingUtils {


  def parsePointsSignDetail(rdd: RDD[String],sqlContext: SQLContext) = {
    //2017-01-05 15:53:35,435 [INFO] root: bi_points|10|7589|2017-01-05 15:53:35|||1||4
    val pointsRdd = rdd.filter(line => (line.contains("bi_points"))).map(line => {
      try {
        val splited = line.split("\\|",-1)
        Row(splited(3),splited(1),splited(2),"",if("".equals(splited(4))) 0 else splited(4).toInt,
          "",splited(6).toDouble,if("".equals(splited(7))) 0.0 else splited(7).toDouble,splited(8).toInt)
      }catch {
        case ex: Exception => {
          Row("0000-00-00 00:00:00","","","",0,"",0,0,0)
        }
        }
    })
    val pointsStruct = (new StructType).add("obtain_time",StringType).add("obtain_id",StringType)
      .add("uid",StringType).add("uname",StringType).add("game_id",IntegerType).add("game_name",StringType)
      .add("points",DoubleType).add("ori_price",DoubleType).add("resourse",IntegerType)
    val orderDF = sqlContext.createDataFrame(pointsRdd, pointsStruct);
    orderDF.registerTempTable("app_points_sign_detail")

    AppStreamingUtils
  }

  def parsePointsRechargeDetail(rdd: RDD[String],sqlContext: SQLContext) = {
    //2017-01-03 22:26:45,842 [INFO] root: bi_order ||W1701031N1026759|1|15844|tc245246939|2017-01-03 22:26:45|294|17|0|6.00|3.00|0.00|0|9||0|0|15844|4|||1|0||v2.2
    val pointsRdd = rdd.filter(line => line.contains("bi_order")&& line.split("\\|",-1).size>=26).map(line => {
      try {
        val splited = line.split("\\|",-1)//26
        Row(splited(6),splited(2),"","",splited(7).toInt,
          splited(5),splited(12).toDouble,splited(12).toDouble,1,splited(19).toInt,splited(25).toLowerCase)
      }catch {
        case ex: Exception => {
          Row("0000-00-00 00:00:00","","","",0,"",0,0,0,0,"")
        }
      }
    })
    val pointsStruct = (new StructType).add("obtain_time",StringType).add("obtain_id",StringType)
      .add("uid",StringType).add("uname",StringType).add("game_id",IntegerType).add("game_name",StringType)
      .add("points",DoubleType).add("ori_price",DoubleType).add("resourse",IntegerType)
      .add("order_state",IntegerType).add("version",StringType)
    val orderDF = sqlContext.createDataFrame(pointsRdd, pointsStruct);
    orderDF.registerTempTable("app_points_recharge_detail")

    AppStreamingUtils
  }

  def getPointFilterRow(row:Row,jedis:Jedis)={

    //flag,obtain_time,obtain_id,uid,uname,game_id,game_name,points,ori_price,resourse
    val obtainTime = row.getString(0)
    val obtainId = row.getString(1)
    var uid = row.getString(2)
    var uname = row.getString(3)
    val gameId = row.getInt(4)
    var gameName = row.getString(5)//getGameName(gameId,jedis)
    var points = row.getDouble(6)
    val oriPrice = row.getDouble(7)
    val resourse = row.getInt(8)

    if(resourse == 1) {

      val integralRedis = jedis.hget("anytimes_recharge","integral")
      val integral = (if(integralRedis==null) "0" else integralRedis).toInt*0.01
      points = points*integral
      if(0<points && points<=1){
        points = 1
      } else {
        points = points.toInt
      }

      val bind = getRechargeBind(gameName,jedis)
      uid = bind._1
      uname = bind._2
      gameName=getGameName(gameId,jedis)
    }
    if(resourse == 4){
      uname = getUname(uid,jedis)
    }

    Row(obtainTime,obtainId,if("".equals(uid)) "0" else uid,uname,gameId,gameName,points,oriPrice,resourse)
  }

  def main(args: Array[String]) {

  }
  def getRechargeBind(gameName:String,jedis:Jedis)={
    var bindId = if("".equals(gameName)) null else jedis.hget(gameName,"bind_member_id")
    bindId = if(bindId == null) "0" else bindId
    val bindName = getUname(bindId,jedis)
    (bindId,bindName)
  }
  /**
   * 获取通行证名称
   * @param uid
   * @return
   */
  def getUname(uid: String,jedis:Jedis): String = {
    val uname = jedis.hget(uid + "_member","username")
    if(uname==null)"" else uname
  }

  /**
   * 获取游戏名称
   * @param gameId
   * @return
   */
  def getGameName(gameId: Int,jedis:Jedis): String = {
    val gameName = jedis.hget(gameId + "_bgame","name")
    if(gameName == null)"" else gameName
  }
}
