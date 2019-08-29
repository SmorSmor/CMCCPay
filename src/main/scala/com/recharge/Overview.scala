package com.recharge

import java.lang

import com.alibaba.fastjson.JSON
import com.utils.JedisOffset
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Overview {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
      // 设置没秒钟每个分区拉取kafka的速率
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      // 设置序列化机制
      .set("spark.serlizer", "org.apache.spark.serializer.KryoSerializer")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    ssc.checkpoint("d://out-cmcc")
    // 配置参数
    // 配置基本参数
    // 组名
    val groupId = "group01"
    // topic
    val topics = Array("cmcc")
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
    var fromOffset: Map[TopicPartition, Long] = JedisOffset(groupId)

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

    // 获取所有数据


    // 指标一
    clacuteOverview(stream)

    // 启动
    ssc.start()
    ssc.awaitTermination()


  }

  /**
    * 1)	统计全网的充值订单量, 充值金额, 充值成功数
    *
    * @param msg
    * @return
    */
  def clacuteOverview(msg: DStream[ConsumerRecord[String, String]]) = {
    val overView: String = "overView"
    val mapedDstream: DStream[(String, (Int, Double, Int))] = msg.map(x => JSON.parseObject(x.value()))
      .filter(obj => obj.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq"))
      .map(jsonObj => {

        val res: String = jsonObj.getString("bussinessRst")
        val fee: Double = if (res.equals("0000")) jsonObj.getString("chargefee").toDouble else 0.0
        val feeCount: Int = if (!fee.equals(0.0)) 1 else 0
        (overView, (1, fee, feeCount))
      })

    val upstateFun = (values: Seq[(Int, Double, Int)], state: Option[(Int, Double, Int)]) => {
      // state存储的是历史批次结果
      // 首先根据state来判断，之前的这个key值是否有对应的值

      var tup: (Int, Double, Int) = (0, 0.0, 0)
      if (state.nonEmpty) tup = state.getOrElse(tup)

      for (value <- values) {
        tup = (tup._1 + value._1, tup._2 + value._2, tup._3 + value._3)
      }

      Option(tup)
    }

    val aggrDStream: DStream[(String, (Int, Double, Int))] = mapedDstream.updateStateByKey(upstateFun)

    aggrDStream.foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        while (it.hasNext) {
          println(it.next())

        }
      })
    })
  }


}
