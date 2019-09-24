package com.Tags

import com.util.HttpUtil
import org.apache.spark.sql.SparkSession
object Test1 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Tags").master("local[*]").getOrCreate()
    val arr=Array("http://restapi.amap.com/v3/geocode/regeo?location=116.310003,39.991957&key=94842f98da803571697880630037ccb1&extensions=all")

    val rdd=spark.sparkContext.makeRDD(arr)
    rdd.map(t=>{
      HttpUtil.get(t)
    })
      .foreach(println)
  }
}
