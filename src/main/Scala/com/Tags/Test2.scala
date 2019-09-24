package com.Tags

import com.util.{AmapUtil, String2Type}
import org.apache.spark.sql.SparkSession

/**
  * 测试工具类
  */
object Test2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Tags").master("local[*]").getOrCreate()
    import spark.implicits._

    // 读取数据文件
    val df = spark.read.parquet("D:\\outputData\\Log2parquet")

    df.map(row=>{
      // 圈
      AmapUtil.getBusinessFromAmap(
        String2Type.toDouble(row.getAs[String]("long")),
        String2Type.toDouble(row.getAs[String]("lat")))
    }).rdd.foreach(println)//返回商圈名字
  }
}
