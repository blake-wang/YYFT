package cn.com.yyft.utils

import java.sql.{DriverManager, Connection}
import scala.collection.mutable.ArrayBuffer

/**
 * Created by sumenghu on 2016/8/9.
 */

object MySqlUtils {
  def getFxConn() : Connection ={
    val url =PropertiesUtils.getRelativePathValue("jdbc.xiaopeng2fx.url")
    val driver = PropertiesUtils.getRelativePathValue("driver")
    try {
      Class.forName(driver)
      val connection = DriverManager.getConnection(url)
      connection
    }
  }


  def getConn(): Connection ={
    val url =PropertiesUtils.getRelativePathValue("url")
    val driver = PropertiesUtils.getRelativePathValue("driver")
    try {
      Class.forName(driver)
      val connection = DriverManager.getConnection(url)
      connection
    }
  }

  def doBatch(sqlText:String,params:ArrayBuffer[Array[Any]],conn:Connection): Unit ={
    val pstat = conn.prepareStatement (sqlText)
    //println("sqlText"+sqlText)
    for(param <- params){
      //println("param: "+param.mkString("##"))
      for(index <- 0 to param.length - 1){
        pstat.setObject(index+1,param(index))
      }
      pstat.addBatch()
    }
    try{
      pstat.executeBatch
    }finally{
      pstat.close
    }
  }











  /*def getValueArray(sql2Mysql:String): Array[String] ={
    val startValuesIndex=sql2Mysql.indexOf("(?")+1
    val endValuesIndex=sql2Mysql.indexOf("?)")+1
    val valueArray:Array[String]=sql2Mysql.substring(startValuesIndex,endValuesIndex).split(",")  //两个（？？）中间的值
    valueArray
  }

  def getCols(sql2Mysql:String): Array[String] ={
    //查找需要insert的字段
    val cols_ref=sql2Mysql.substring(0,sql2Mysql.lastIndexOf("(?"))  //获取（?特殊字符前的字符串，然后再找字段
    val cols:Array[String]=cols_ref.substring(cols_ref.lastIndexOf("(")+1,cols_ref.lastIndexOf(")")).split(",")
    cols
  }

  def getWh(sql2Mysql:String): Array[String] ={
    //条件中的参数个数
    val wh:Array[String]=sql2Mysql.substring(sql2Mysql.indexOf("update")+6).split(",")  //找update后面的字符串再判断
    wh
  }

  def insertOrUpdateDate(rows:Iterator[Row],sql2Mysql:String): Unit ={
    val conn = getConn()
    val pstat = conn.prepareStatement (sql2Mysql)

    //values中的个数
    val valueArray:Array[String] = getValueArray(sql2Mysql)  //两个（？？）中间的值

    val cols = getCols(sql2Mysql)

    //条件中的参数个数
    val wh:Array[String]= getWh(sql2Mysql)

    for (x <- rows){
      //补充value值
      for (rs <- 0 to valueArray.length - 1) {
        pstat.setString(rs.toInt + 1, x.get(rs).toString)
      }
      //补充条件
      if (wh.length > 0)
        for (i <- 0 to wh.length - 1) {
          val rs = wh(i).trim.substring(0, wh(i).trim.lastIndexOf("="))
          for (ii <- 0 to cols.length - 1) {
            if (cols(ii).trim.equals(rs)) {
              pstat.setString(i.toInt + valueArray.length.toInt + 1, x.get(ii).toString)
            }
          }
        }
    }
    try{
      pstat.executeBatch
    }finally{
      pstat.close
      conn.close
    }
  }*/

}
