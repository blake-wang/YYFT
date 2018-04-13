package cn.com.yyft.mllib

import cn.com.yyft.utils.MySqlUtils
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by JSJSB-0071 on 2017/1/4.
 */
object DownCf {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("DownloadCF")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    //2017-01-03 14:28:32,556 [INFO] root: bi_appdownload|298|18|865586029885631|android|PE-TL20|wifi|10.1.1.198|2017-01-03 14:28:32|13239|com.youzu.jz.baidu.ad|
    val ratings = sc.textFile("file:///E:\\project\\YYFT\\src\\cn\\com\\yyft\\mllib\\test.txt").map(line => {
      val splited = line.split("\\|", -1);
      //用户，商品，评分：imei，游戏id，评分
      Rating(splited(3).hashCode(), splited(1).toInt, 1)
    })
    val splits = ratings.randomSplit(Array(0.8, 0.2))
    val training = splits(0).cache()
    val test = splits(0).cache()

    /* val ranks = List(5,10,15,20)
     val lambdas = List(0.01,0.1,1.0,10.0)
     val iters = List(10,25,50,75,100)*/

    val ranks = List(5, 10)
    val lambdas = List(0.01, 0.1)
    val iters = List(10, 25)

    var bestRank = 0
    var bestLambda = -1.0
    var bestIter = -1

    var bestMode: MatrixFactorizationModel = null
    var bestValidateRnse = Double.MaxValue
    for (rank <- ranks; lam <- lambdas; iter <- iters) {
      val model = new ALS()
        .setRank(rank)
        .setIterations(iter)
        .setLambda(lam)
        .run(training)
      val rmse = computeRmse(model, test)

      if (rmse < bestValidateRnse) {
        bestMode = model
        bestValidateRnse = rmse
        bestRank = rank
        bestLambda = lam
        bestIter = iter
      }
      //println("--最优参数："+"rank="+rank+" lam="+lam+" iter="+iter)
    }
    //println("最优参数："+"bestRank="+bestRank+" bestLambda="+bestLambda+" bestIter="+bestIter)
    //bestMode.recommendProducts("869157022386781".hashCode, 40).foreach(println _); //3126308

    val resultRdd = ratings.map(rating => {
      val gameId = rating.product
      val imei = rating.user
      val hashImei = imei.hashCode()
      val recommendP = bestMode.recommendProducts(hashImei, 60);
      var gameIdRs = ""
      recommendP.sortBy(f => f.rating > f.rating).foreach(r => {
        if(gameId != r.product) {
          gameIdRs = r.product + "|" + gameIdRs
        }
      })
      gameIdRs = gameIdRs.substring(0,gameIdRs.length-1)
      (gameId,gameIdRs)
    })

    resultRdd.foreachPartition(par => {
      val conn = MySqlUtils.getConn()

      val sqlText = " insert into bi_app_game_recommend(game_id,game_id_rs)" +
        " values(?,?) on duplicate key update game_id_rs=?"
      val params = new ArrayBuffer[Array[Any]]()
      for (r <- par) {
        params.+=(Array[Any](r._1,r._2,r._1))
      }
      try {
        MySqlUtils.doBatch(sqlText, params, conn)
      } finally {
        conn.close
      }
    })
    /*ratings.foreachPartition(ratings => {
      val conn = MySqlUtils.getConn()

      val sqlText = " insert into bi_app_game_recommend(game_id,game_id_rs)" +
        " values(?,?) on duplicate key update game_id_rs=?"
      val params = new ArrayBuffer[Array[Any]]()
      for (rating <- ratings) {
        val gameId = rating.product
        val imei = rating.user
        val hashImei = imei.hashCode()
        val recommendP = bestMode.recommendProducts(hashImei, 60);
        var gameIdRs = ""
        recommendP.sortBy(f => f.rating > f.rating).foreach(r => {
          if(gameId != r.product) {
            gameIdRs = r.product + "|" + gameIdRs
          }
        })
        gameIdRs = gameIdRs.substring(0,gameIdRs.length-1)
        params.+=(Array[Any](gameId,gameIdRs,gameId))
      }
      try {
        MySqlUtils.doBatch(sqlText, params, conn)
      } finally {
        conn.close
      }
    })*/
    sc.stop()
  }

  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {

    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map { x =>
      ((x.user, x.product), x.rating)
    }.join(data.map(x => ((x.user, x.product), x.rating))).values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
  }

}

