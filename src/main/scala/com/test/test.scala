package com.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("C:\\Users\\孙洪斌\\Desktop\\reduced-tweets.json")
    df.show()

    spark.stop()
  }

}
