package com.recharge

import java.lang
import java.sql.{Connection, ResultSet, Statement}

import com.alibaba.fastjson.JSON
import com.utils.{DBConnectionPool, JedisConnectionPool, JedisOffset, TimeUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs


/**
  * Redis管理Offset
  */
object Overview {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("offset").setMaster("local[2]")
      // 设置没秒钟每个分区拉取kafka的速率
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      // 设置序列化机制
      .set("spark.serlizer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf, Seconds(1))
    // 配置参数
    // 配置基本参数
    // 组名
    val groupId = "group01"
    // topic
    val topic = "cmcc_pay"
    // 指定Kafka的broker地址（SparkStreaming程序消费过程中，需要和Kafka的分区对应）
    val brokerList = "hadoop01:9092,hadoop02:9092,hadoop03:9092"
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
      if (fromOffset.size == 0) {
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

    // 将城市编号和城市名做成 MAP 广播出去
    val AppParams = ssc.sparkContext.textFile("C:\\Users\\孙洪斌\\Desktop\\充值平台实时统计分析\\city.txt")
      .map(_.split(" ")).map(arr => (arr(0), arr(1))).collect().toMap
    val pcode2PName = ssc.sparkContext.broadcast(AppParams)
    stream.foreachRDD({
      rdd =>
        val offestRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 业务处理
        val baseRDD = rdd.map(t => JSON.parseObject(t.value()))
          // 过滤接口
          .filter(_.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq"))
          .map(obj => {
            // 判断该条日志是否是充值成功的日志
            val result = obj.getString("bussinessRst")
            val fee = obj.getDouble("chargefee")

            // 充值发起时间和结束时间
            val requestId = obj.getString("requestId")
            val receiveTime = obj.getString("receiveNotifyTime")
            // 数据当前日期
            val day = requestId.substring(0, 8)
            val hour = requestId.substring(0, 10)
            val minute = requestId.substring(0, 12)

            // 省份Code
            val provinceCode = obj.getString("provinceCode")

            // 花费时间
            val costTime = TimeUtil.caculateTime(requestId, receiveTime)
            val succAndFeeAndTime: (Double, Double, Double) = if (result.equals("0000")) (1, fee, costTime) else (0, 0, 0)

            // (日期, 小时, 分钟，Kpi(订单数，成功订单，订单金额，订单时长)，编号省份)
            (day, hour, minute, List[Double](1, succAndFeeAndTime._1, succAndFeeAndTime._2, succAndFeeAndTime._3), provinceCode)

          }).cache()

        // 指标 1.1	统计全网的充值订单量, 充值金额, 充值成功数
        baseRDD.map(tp => (tp._1, tp._4)).reduceByKey((list1, list2) => {
          // 聚合结果
          list1.zip(list2).map(tp => tp._1 + tp._2)
        }).foreachPartition(part => {
          val jedis = JedisConnectionPool.getConnection()
          part.foreach(tp => {
            // hincrBy如果有就累加，如果没有就插入
            jedis.hincrBy("A-" + tp._1, "total", tp._2.head.toLong)
            jedis.hincrBy("A-" + tp._1, "succ", tp._2(1).toLong)
            jedis.hincrByFloat("A-" + tp._1, "money", tp._2(2))
            jedis.hincrBy("A-" + tp._1, "cost", tp._2(3).toLong)
            jedis.expire("A-" + tp._1, 48 * 60 * 60) // 设置key的有效期
            // 输出控制台测试
            println(jedis.hget("A-" + tp._1, "total"))
            println(jedis.hget("A-" + tp._1, "succ"))
            println(jedis.hget("A-" + tp._1, "money"))
            println(jedis.hget("A-" + tp._1, "cost"))
            println("-------------------------------------")
          })
          jedis.close()
        })

        //        // 指标 1.2 实时充值业务办理趋势, 主要统计全网每分钟的订单量数据
        //        baseRDD.map(tp => ((tp._1, tp._2, tp._3), List(tp._4.head, tp._4(1)))).foreachPartition(part => {
        //          val jedis = JedisConnectionPool.getConnection()
        //          part.foreach(tp => {
        //
        //            //总的充值成功和失败订单数量
        //            jedis.hincrBy("B-" + tp._1._1, "T:" + tp._1._2 + tp._1._3, tp._2.head.toLong)
        //            jedis.hincrBy("B-" + tp._1._1, "S:" + tp._1._2 + tp._1._3, tp._2(1).toLong) //充值成功的订单数量
        //            jedis.expire("B-" + tp._1._1, 48 * 60 * 60)
        //            // 控制台输出测试
        //            println(jedis.hget("B-" + tp._1._1, "T:" + tp._1._2 + tp._1._3))
        //            println(jedis.hget("B-" + tp._1._1, "S:" + tp._1._2 + tp._1._3)) //充值成功的订单数量
        //            println("-------------------------------------")
        //          })
        //          jedis.close()
        //        })

        // 指标 2 统计每小时各个省份的充值失败数据量

        baseRDD.map(tp => ((tp._2, tp._5), (tp._4.head, tp._4(1))))
          .foreachPartition(part => {
            val connection: Connection = DBConnectionPool.getConn()
            val statement: Statement = connection.createStatement()
            part.foreach(tp => {
              // 每个省每小时失败的个数

              if (tp._2._2 == 0) {
                val res: ResultSet = statement.executeQuery(s"select city from city_failcount where city = '${pcode2PName.value.getOrElse(tp._1._2, tp._1._2)}' and time = '${tp._1._1}' ")
                if (!res.next()) {
                  statement.execute(s"insert into city_failcount values('${pcode2PName.value.getOrElse(tp._1._2, tp._1._2)}','${tp._1._1}','${1}')")
                } else {
                  statement.execute(s"UPDATE city_failcount SET count  = count + 1 where  city = '${pcode2PName.value.getOrElse(tp._1._2, tp._1._2)}' and time = '${tp._1._1}'")
                }
              }
            })
            DBConnectionPool.releaseCon(connection)
          })


        //        // 指标 3 以省份为维度统计订单量排名前 10 的省份数据,并且统计每个省份的订单成功率，只保留一位小数，存入MySQL中，进行前台页面显示。
        //        baseRDD.map(tp => (tp._5, (tp._4.head, tp._4(1))))
        //          .foreachPartition(part => {
        //            DBs.setupAll()
        //            part.foreach(tp => {
        //
        //              // 如果有就累加，如果没有就插入
        //              val maybeString: Option[String] = DB.readOnly(implicit session => {
        //                SQL(s"select city from city_topn where city = '${pcode2PName.value.getOrElse(tp._1, tp._1)}' ")
        //                  .map(rs => rs.string("city"))
        //                  .first.apply()
        //              })
        //              if (maybeString.isEmpty) {
        //                DB.localTx(implicit session => {
        //                  SQL("insert into city_topn values(?,?,?)")
        //                    .bind(pcode2PName.value.getOrElse(tp._1, tp._1), tp._2._2, (tp._2._2.toDouble / tp._2._1.toDouble))
        //                    .update().apply()
        //                })
        //              } else {
        //                DB.localTx { implicit session =>
        //                  SQL("UPDATE city_topn SET count  = count + ? , succ = ? where city = ?")
        //                    .bind(tp._2._2, (tp._2._2.toDouble / tp._2._1.toDouble), pcode2PName.value.getOrElse(tp._1, tp._1))
        //                    .update().apply()
        //                }
        //              }
        //            })
        //          })
        //
        //        // 指标 4 实时统计每小时的充值笔数和充值金额。
        //
        //        baseRDD.map(tp => (tp._2, (tp._4(1), tp._4(2))))
        //          .foreachPartition(part => {
        //            DBs.setupAll()
        //            part.foreach(tp => {
        //
        //              // 如果有就累加，如果没有就插入
        //              val maybeString: Option[String] = DB.readOnly(implicit session => {
        //                SQL(s"select hour from hour_countcast where hour = '${tp._1}' ")
        //                  .map(rs => rs.string("hour"))
        //                  .first.apply()
        //              })
        //              if (maybeString.isEmpty) {
        //                DB.localTx(implicit session => {
        //                  SQL("insert into hour_countcast values(?,?,?)")
        //                    .bind(tp._1, tp._2._1, tp._2._2)
        //                    .update().apply()
        //                })
        //              } else {
        //                DB.localTx { implicit session =>
        //                  SQL("update hour_countcast set count  = count + ? , cast = cast + ? where hour = ?")
        //                    .bind(tp._2._1, tp._2._2, tp._1)
        //                    .update().apply()
        //                }
        //              }
        //            })
        //          })


        // 将偏移量进行更新
        val jedis = JedisConnectionPool.getConnection()
        for (or <- offestRange) {
          jedis.hset(groupId, or.topic + "-" + or.partition, or.untilOffset.toString)
        }
        jedis.close()
    })
    // 启动
    ssc.start()
    ssc.awaitTermination()
  }


}
