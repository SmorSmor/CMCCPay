package com.exam

import org.apache.spark.{SparkConf, SparkContext}

object Ip2Provience {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val buffer = collection.mutable.ListBuffer[(Long, String)]()
    val list: List[(Long, String)] = sc.textFile("C:\\Users\\孙洪斌\\Desktop\\考试\\ip.txt")
      .map(_.split("\\|"))
      .map(arr => {
        val ip1 = arr(0)
        val provience = arr(6)
        (ip2Long(ip1), provience)
      }).collect().toList



    println(list)


    sc.stop()

  }

  def getProvience(list: List[(Long, String)], ip: String) = {
    val tar: Long = ip2Long(ip)
    var min = 0
    var max = list.length - 1
    var mid = 0
    while (min <= max) {
      mid = (max + min) / 2
      val value1 = list(mid)._1
      if (tar == value1)
        mid
      else if (tar < value1)
        max = mid - 1
      else
        min = mid + 1
    }
    list(mid)._2

  }

  def ip2Long(ip: String): Long = {
    val s = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until s.length) {
      ipNum = s(i).toLong | ipNum << 8L
    }
    ipNum
  }


}
