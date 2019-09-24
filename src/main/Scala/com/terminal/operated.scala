package com.terminal
import com.util.RptUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
//ispid: Int,	运营商 id
//ispname: String,	运营商名称
object operated {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val opeDf: DataFrame = spark.read.parquet("D:\\outputData\\Log2parquet")
    opeDf.rdd.map(line=>{

      var ispname = line.getAs[String]("ispname")
      if(ispname=="未知"){ //如果没有应用名称就用数据字典中的appid对应的代替value值appname代替
        ispname="其他"   //注意，这里不能用val  ispname，这相当于重新定义，重复定义
      }

//      var ispname=""
//      if( line.getAs[String]("ispid")==4){
//        ispname="其他"
//      }
//      else
//        ispname=line.getAs[String]("ispname")

      val requestmode: Int = line.getAs[Int]("requestmode")
      val processnode: Int = line.getAs[Int]("processnode")
      val iseffective: Int = line.getAs[Int]("iseffective")
      val isbilling: Int = line.getAs[Int]("isbilling")
      val isbid: Int = line.getAs[Int]("isbid")
      val iswin: Int = line.getAs[Int]("iswin")
      val adorderid: Int = line.getAs[Int]("adorderid")
      val winprice: Double = line.getAs[Double]("winprice")
      val adpayment: Double = line.getAs[Double]("adpayment")

      val reqNum: List[Double] = RptUtils.ReqPt(requestmode,processnode)

      val cliNum: List[Double] = RptUtils.clickPt(requestmode,iseffective)

      val adNum: List[Double] = RptUtils.adPt(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)

      val allList: List[Double] = reqNum ++ cliNum ++ adNum

      (ispname,allList)
    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    }).map(t=>t._1+","+t._2.mkString(",")).saveAsTextFile("D:\\outputData\\operated")

  }
}
//已运行出结果

