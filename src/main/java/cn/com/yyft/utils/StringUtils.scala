package cn.com.yyft.utils

/**
 * Created by sumenghu on 2016/9/1.
 */
object StringUtils {

  /**
   * 判断字符串是否符合正则表达式
   * @param log
   * @param regx
   * @return
   */
  def isRequestLog(log:String,regx:String) :Boolean={
    val p1 = regx.r
    val p1Matches = log match {
      case p1() => true // no groups
      case _ => false
    }
    p1Matches
  }

  def defaultEmptyTo21(str:String):String={
    if("".equals(str)){
      "21"
    }else if ("\\N".equals(str)){
      "pyw"
    }else{
      str
    }
  }

  def getArrayChannel(channelId:String): Array[String] ={
    val splited = channelId.split("_")
    if (channelId == null || channelId.equals("") || channelId.equals("0")) {
      Array[String]("21", "", "")
    } else if (splited.length < 3) {
      Array[String](channelId, "", "")
    } else {
      Array[String](splited(0), splited(1), splited(2))
    }
  }
}
