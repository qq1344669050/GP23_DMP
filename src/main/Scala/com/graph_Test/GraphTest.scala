package com.graph_Test

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

/**
  * 图计算案例（好友关联推荐）
  */
object GraphTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("graph").master("local").getOrCreate()

    // 创建点和边
    // 构建点的集合
    val vertexRDD = spark.sparkContext.makeRDD(Seq(
      (1L,("小明",26)),
      (2L,("小红",30)),
      (6L,("小黑",33)),
      (9L,("小白",26)),
      (133L,("小黄",30)),
      (138L,("小蓝",33)),
      (158L,("小绿",26)),
      (16L,("小龙",30)),
      (44L,("小强",33)),
      (21L,("小胡",26)),
      (5L,("小狗",30)),
      (7L,("小熊",33))
    ))
    // 构造边的集合
    val edgeRDD = spark.sparkContext.makeRDD(Seq(
      Edge(1L,133L,0),
      Edge(2L,133L,0),
      Edge(6L,133L,0),
      Edge(9L,133L,0),
      Edge(6L,138L,0),
      Edge(16L,138L,0),
      Edge(21L,138L,0),
      Edge(44L,138L,0),
      Edge(5L,158L,0),
      Edge(7L,158L,0)
    ))

    // 构建图
    val graph = Graph(vertexRDD,edgeRDD)
    // 取顶点
    val vertices = graph.connectedComponents().vertices//.foreach(println())打印测试看点的情况，哪个点最小哪个是顶点，负数也算
println(vertices.collect().toBuffer)


    // 匹配数据
    vertices.join(vertexRDD).map{
      case (userId,(cnId,(name,age)))=>(cnId,List((name,age)))
    }
      .reduceByKey(_++_)//这里只是把list变成一个
      .foreach(println)
  }
}

//运行结果1:ArrayBuffer((21,1), (16,1), (158,5), (138,1), (133,1), (1,1), (6,1), (7,5), (9,1), (44,1), (5,5), (2,1))

//运行结果2:
// ArrayBuffer((21,(1,(小胡,26))), (16,(1,(小龙,30))), (158,(5,(小绿,26))), (138,(1,(小蓝,33))),
// (133,(1,(小黄,30))), (1,(1,(小明,26))), (6,(1,(小黑,33))), (7,(5,(小熊,33))), (9,(1,(小白,26))),
// (44,(1,(小强,33))), (5,(5,(小狗,30))), (2,(1,(小红,30))))

//运行结果3:各连通图最小顶点为key的连通点的用户信息有哪些
//(1,List((小胡,26), (小龙,30), (小蓝,33), (小黄,30), (小明,26), (小黑,33), (小白,26), (小强,33), (小红,30)))
//(5,List((小绿,26), (小熊,33), (小狗,30)))
