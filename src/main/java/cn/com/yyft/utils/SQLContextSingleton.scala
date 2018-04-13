package cn.com.yyft.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by sumenghu on 2016/9/27.
  */
object SQLContextSingleton {
  @transient private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
