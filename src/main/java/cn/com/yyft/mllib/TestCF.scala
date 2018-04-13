package cn.com.yyft.mllib

/**
  * Created by JSJSB-0071 on 2016/12/7.
  */
object TestCF {
  def main(args: Array[String]) {
    val ranks = List(8, 22)
    val lambdas = List(0.1, 10.0)

    for (rank <- ranks) println(rank)
  }
}
