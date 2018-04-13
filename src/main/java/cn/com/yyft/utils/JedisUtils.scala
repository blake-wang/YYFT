package cn.com.yyft.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * Created by JSJSB-0071 on 2016/10/25.
  */

object JedisUtils {

  def getPool() = {
    val host = PropertiesUtils.getRelativePathValue("redis.host")
    val port = Integer.parseInt(PropertiesUtils.getRelativePathValue("redis.port"))
    val poolConfig = new GenericObjectPoolConfig();
    //最大空闲连接数
    poolConfig.setMaxIdle(Integer.parseInt(PropertiesUtils.getRelativePathValue("redis.max.idle")))
    //连接池的最大连接数
    poolConfig.setMaxTotal(Integer.parseInt(PropertiesUtils.getRelativePathValue("redis.max.total")))
    //设置获取连接的最大等待时间
    poolConfig.setMaxWaitMillis(Integer.parseInt(PropertiesUtils.getRelativePathValue("redis.max.wait.millis")))
    //从连接池中获取连接的时候是否需要校验，这样可以保证取出的连接都是可用的
    poolConfig.setTestOnBorrow(true)
    //获取jedis连接池
    val pool = new JedisPool(poolConfig, host, port, 30000, "redis");
    pool
  }


  /*val hook = new Thread {
   override def run = {
     println("Execute hook thread: " + this)
     pool.destroy()
   }

 }
 sys.addShutdownHook(hook.run)*/

}
