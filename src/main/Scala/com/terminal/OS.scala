package com.terminal

import com.util.RptUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
//client: Int,	设备类型 （1：android 2：ios 3：wp）
object OS {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val netDf: DataFrame = spark.read.parquet("D:\\outputData\\Log2parquet")
    netDf.rdd.map(line=>{
      var clientName=""
      if(line.getAs[Int]("client")==1){
        clientName="android"
      }
      else if(line.getAs[Int]("client")==2)
        clientName="ios"
      else
        clientName="其他"

      val requestmode = line.getAs[Int]("requestmode")
      val processnode = line.getAs[Int]("processnode")
      val iseffective = line.getAs[Int]("iseffective")
      val isbilling = line.getAs[Int]("isbilling")
      val isbid = line.getAs[Int]("isbid")
      val iswin = line.getAs[Int]("iswin")
      val adorderid= line.getAs[Int]("adorderid")
      val winprice = line.getAs[Double]("winprice")
      val adpayment = line.getAs[Double]("adpayment")

      val reqNum: List[Double] = RptUtils.ReqPt(requestmode,processnode)
      val cliNum: List[Double] = RptUtils.clickPt(requestmode,iseffective)
      val adNum: List[Double] = RptUtils.adPt(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)

      val allList: List[Double] = reqNum++cliNum++adNum

      (clientName,allList)
    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    }).map(t=>t._1+","+t._2.mkString(",")).saveAsTextFile("D:\\outputData\\OS")
  }
}
