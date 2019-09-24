package com.App

import com.util.RptUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession

/**媒体分析指标 */
class APP {
}

object APP {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // 读取数据字典
    val docMap = spark.sparkContext.textFile("D:\\inputData\\app_dict.txt").map(_.split("\\s",-1))
      .filter(_.length>=5).map(arr=>(arr(4),arr(1))).collectAsMap()//arr(4)是url,arr(1)是名字
    // 进行广播
    val broadcast = spark.sparkContext.broadcast(docMap)
    // 读取数据文件
    val df = spark.read.parquet("D:\\outputData\\Log2parquet")
    df.rdd.map(row=>{
      //取媒体相关字段
      var appName = row.getAs[String]("appname")
      if(StringUtils.isBlank(appName)){ //如果没有应用名称就用数据字典中的appid对应的代替value值appname代替
        appName = broadcast.value.getOrElse(row.getAs[String]("appid"),"unknow")
      }
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid= row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      // 处理请求数
      val rptList = RptUtils.ReqPt(requestmode,processnode)
      // 处理展示点击
      val clickList = RptUtils.clickPt(requestmode,iseffective)
      // 处理广告
      val adList = RptUtils.adPt(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)
      // 所有指标
      val allList:List[Double] = rptList ++ clickList ++ adList
      (appName,allList)
    }).reduceByKey((list1,list2)=>{
      // list1(1,1,1,1).zip(list2(1,1,1,1))=list((1,1),(1,1),(1,1),(1,1))
      list1.zip(list2).map(t=>t._1+t._2)
    })
      .map(t=>t._1+","+t._2.mkString(","))
      .saveAsTextFile("D:\\outputData\\APP")

  }
}