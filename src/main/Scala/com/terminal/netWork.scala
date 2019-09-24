package com.terminal

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.util.RptUtils
import scala.tools.scalap.scalax.util.StringUtil
//29	networkmannerid: Int,	联网方式 id
//30	networkmannername:
//String,	联网方式名称
object netWork {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val netDf: DataFrame = spark.read.parquet("D:\\outputData\\Log2parquet")
    netDf.rdd.map(line=>{
      var networkmannername=line.getAs[String]("networkmannername")
      if(networkmannername=="未知"){
        networkmannername="其他"
      }

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

      (networkmannername,allList)
    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    }).map(t=>t._1+","+t._2.mkString(",")).saveAsTextFile("D:\\outputData\\netWork")

  }
}
