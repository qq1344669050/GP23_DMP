package com.ProCityCt

import org.apache.spark.sql.SparkSession

/* 统计省市指标 */
object ProCityCt {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // 获取数据
    val df = spark.read.parquet("D:\\outputData\\Log2parquet")
    // 注册临时视图
    df.createTempView("log")
    val df2 = spark
      .sql("select provincename,cityname,count(*) ct from log group by provincename,cityname")

    df2.write.partitionBy("provincename","cityname").json("D:\\outputData\\ProCityCt")
    // 存Mysql

    // 通过config配置文件依赖进行加载相关的配置信息
//    val load = ConfigFactory.load()
//    // 创建Properties对象
//    val prop = new Properties()
//    prop.setProperty("user",load.getString("jdbc.user"))
//    prop.setProperty("password",load.getString("jdbc.password"))
//    // 存储
//    df2.write.mode(SaveMode.Append).jdbc(     //df2是有数据和结构的信息
//      load.getString("jdbc.url"),load.getString("jdbc.tablName"),prop)

    spark.stop()
  }
}
