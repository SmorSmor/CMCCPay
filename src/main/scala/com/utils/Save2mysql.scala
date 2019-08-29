package com.utils

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Save2mysql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val df: DataFrame = spark.read.json("C:\\Users\\孙洪斌\\Desktop\\充值平台实时统计分析\\cmcc.json")

    val config = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user", config.getString("jdbc.user"))
    prop.setProperty("password", config.getString("jdbc.password"))
    df.write.mode(SaveMode.Append).jdbc(config.getString("jdbc.url"),"cmcc", prop)

  }

}
