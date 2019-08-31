package com.test

import com.utils.JedisConnectionPool
import redis.clients.jedis.Jedis

object test00 {
  def main(args: Array[String]): Unit = {
    val jedis: Jedis = JedisConnectionPool.getConnection()

    jedis.select(15)

    jedis.set("wuxaing:nanshen:wudishuai","niubi")
    jedis.set("wuxaing:erzi:wudishuai","niubi")
    jedis.set("wuxaing:erzi:wudi","niubi")
    println("插入成功")

    Thread.sleep(5000)
    jedis.del("wuxaing:erzi:wudishuai")
    println("删除一条")

    Thread.sleep(5000)
    jedis.del("wuxaing:erzi:wudi")

    jedis.close()

    println("操作完成")

  }

}
