package com.exam

import java.lang

import com.utils.{JedisConnectionPool, JedisOffset}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Redis管理Offset
  */
object exam01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("offset").setMaster("local[2]")
      // 设置没秒钟每个分区拉取kafka的速率
      .set("spark.streaming.kafka.maxRatePerPartition", "2")
      // 设置序列化机制
      .set("spark.serlizer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf, Seconds(3))
    // 配置参数
    // 配置基本参数
    // 组名
    val groupId = "group01"
    // topic
    val topic = "exam01"
    // 指定Kafka的broker地址（SparkStreaming程序消费过程中，需要和Kafka的分区对应）
    val brokerList = "hadoop00:9092"
    // 编写Kafka的配置参数
    val kafkas = Map[String, Object](
      "bootstrap.servers" -> brokerList,
      // kafka的Key和values解码方式
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      // 从头消费
      "auto.offset.reset" -> "earliest",
      // 不需要程序自动提交Offset
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    // 创建topic集合，可能会消费多个Topic
    val topics = Set(topic)
    // 第一步获取Offset
    // 第二步通过Offset获取Kafka数据
    // 第三步提交更新Offset
    // 获取Offset
    var fromOffset: Map[TopicPartition, Long] = JedisOffset(groupId)
    // 判断一下有没数据
    val stream: InputDStream[ConsumerRecord[String, String]] =
      if (fromOffset.isEmpty) {
        KafkaUtils.createDirectStream(ssc,
          // 本地策略
          // 将数据均匀的分配到各个Executor上面
          LocationStrategies.PreferConsistent,
          // 消费者策略
          // 可以动态增加分区
          ConsumerStrategies.Subscribe[String, String](topics, kafkas)
        )
      } else {
        // 不是第一次消费
        KafkaUtils.createDirectStream(
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Assign[String, String](fromOffset.keys, kafkas, fromOffset)
        )
      }


    val list: List[(Long, String)] = ssc.sparkContext.textFile("C:\\Users\\孙洪斌\\Desktop\\考试\\ip.txt")
      .map(_.split("\\|"))
      .map(arr => {
        val ip1 = arr(0)
        val provience = arr(6)
        (ip2Long(ip1), provience)
      }).collect().toList


    val bd: Broadcast[List[(Long, String)]] = ssc.sparkContext.broadcast(list)
    stream.foreachRDD({
      rdd =>
        val offestRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 业务处理
        val baseRDD: RDD[(String, String, String, String, Long)] = rdd.map(_.value()).map(_.split(" "))
          .map(arr => {
            val time: String = arr(0)
            val ip: String = arr(1)
            val goodsType: String = arr(2)
            val detail: String = arr(3)
            val money: Long = arr(4).toLong
            (time, ip, goodsType, detail, money)

          }).cache()

        // 问题1.计算出总的成交量总额（结果保存到Redis中）

        baseRDD.map(tp => tp._5)
          .foreachPartition(part => {
            val jedis = JedisConnectionPool.getConnection()
            part.foreach(tp => {
              jedis.select(2)
              jedis.hincrBy("exam01:总额", "total", tp)
            })
            jedis.close()
          })


        // 问题2.计算每个商品分类的成交量的（结果保存到Redis中）
        baseRDD.map(tp => (tp._3, tp._5))
          .foreachPartition(part => {
            val jedis = JedisConnectionPool.getConnection()
            part.foreach(tp => {
              jedis.select(2)
              jedis.hincrBy("exam01:类型", "type:" + tp._1, tp._2)
            })
            jedis.close()
          })



        // 问题3.计算每个地域的商品成交总金额（结果保存到Redis中）
        baseRDD.map(tp => (getProvience(bd.value, tp._2), tp._5))
          .map(t => (list(t._1)._2, t._2))
          .foreachPartition(part => {
            val jedis = JedisConnectionPool.getConnection()
            part.foreach(tp => {
              jedis.select(2)
              jedis.hincrBy("exam01:省份", "province:" + tp._1, tp._2)
            })
            jedis.close()
          })


        // 将偏移量进行更新
        val jedis = JedisConnectionPool.getConnection()
        for (or <- offestRange) {
          jedis.select(2)
          jedis.hset(groupId, or.topic + "-" + or.partition, or.untilOffset.toString)
        }
        jedis.close()
    })
    // 启动
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 通过ip获取省份
    *
    * @param list
    * @param ip
    * @return
    */
  def getProvience(list: List[(Long, String)], ip: String): Int = {
    val tar: Long = ip2Long(ip)
    var min = 0
    var max = list.length - 1
    var mid = 0
    while (min <= max) {
      mid = (max + min) / 2
      val value1 = list(mid)._1
      val value2 = list(mid + 1)._1
      if (tar >= value1 && tar < value2)
        return mid
      else if (tar >= value2)
        min = mid + 1
      else
        max = mid - 1
    }
    mid

  }

  /**
    * ip格式转为Long
    *
    * @param ip
    * @return
    */
  def ip2Long(ip: String): Long = {
    val s = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until s.length) {
      ipNum = s(i).toLong | ipNum << 8L
    }
    ipNum
  }
}
