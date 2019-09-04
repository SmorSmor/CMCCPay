package dianshang

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkHive {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val config = ConfigFactory.load("shop_dm.properties")
    val load = ConfigFactory.load("node_mysql.properties")


    val prop = new Properties()
    prop.put("user", load.getString("jdbc.user"))
    prop.put("password", load.getString("jdbc.password"))
    val url = load.getString("jdbc.url")

//    // 行为日志浏览类别统计
//    // 查询生成DF
//    val shop_dm_actlog_view = spark.sql(config.getString("shop_dm_actlog_view"))
//    // 写入HIVE
//    shop_dm_actlog_view.write.mode(SaveMode.Overwrite).insertInto("dm_shop.dm_actlog_view")
//    // 写入MySQL
//    shop_dm_actlog_view.write.mode("append").jdbc(url, "shop_dm_actlog_view", prop)


    // 用户浏览地域统计
    val dm_actlog_view_region = spark.sql(config.getString("dm_actlog_view_region"))
    // 写入HIVE
    dm_actlog_view_region.write.mode(SaveMode.Overwrite).insertInto("dm_shop.dm_actlog_view_region")
    // 写入MySQL
    dm_actlog_view_region.write.mode("append").jdbc(url, "dm_actlog_view_region", prop)


    spark.stop()


  }

}
