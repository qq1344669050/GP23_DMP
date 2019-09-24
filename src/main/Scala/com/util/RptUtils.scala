package com.util

/**处理指标统计工具类 */
object RptUtils {

  // 处理请求数
  def ReqPt(requestmode:Int,processnode:Int):List[Double]={
    if(requestmode ==1 && processnode ==1){
      List[Double](1,0,0)//因为一行原始数据就是一条请求，所以用1打标签
    }else if(requestmode ==1 && processnode ==2){
      List[Double](1,1,0)
    }else if(requestmode ==1 && processnode ==3){
      List[Double](1,1,1)
    }else{
      List[Double](0,0,0)
    }
  }
  // 处理点击展示数
  def clickPt(requestmode:Int,iseffective:Int):List[Double]={
    if(requestmode ==2 && iseffective ==1){
      List[Double](1,0)
    }else if(requestmode ==3 && iseffective ==1){
      List[Double](0,1)
    }else{
      List[Double](0,0)
    }
  }
  // 处理参与竞价数，竞价成功数，广告成本和广告消费
  def adPt(iseffective:Int,isbilling:Int,
           isbid:Int,iswin:Int,adorderid:Int,winprice:Double,adpayment:Double):List[Double]={
    if(iseffective ==1 && isbilling ==1 && isbid ==1){
      if(iseffective ==1 && isbilling ==1 && iswin ==1 && adorderid !=0){
        List[Double](1,1,winprice/1000.0,adpayment/1000.0)
      }else{
        List[Double](1,0,0,0)
      }
    }else{
      List[Double](0,0,0,0)
    }

  }
}
