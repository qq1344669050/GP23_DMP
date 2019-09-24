package com.Tags

import com.typesafe.config.ConfigFactory
import com.util.TagUtils
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

/**
  * 上下文标签主类
  */
object TagsContext2 {

  def main(args: Array[String]): Unit = {

    // 创建Spark上下文
    val spark = SparkSession.builder().appName("Tags").master("local").getOrCreate()
    import spark.implicits._

//    // 调用HbaseAPI
//    val load = ConfigFactory.load()
//    // 获取表名
//    val HbaseTableName = load.getString("HBASE.tableName")
//    // 创建Hadoop任务
//    val configuration = spark.sparkContext.hadoopConfiguration
//    // 配置Hbase连接
//    configuration.set("hbase.zookeeper.quorum",load.getString("HBASE.Host"))
//    // 获取connection连接
//    val hbConn = ConnectionFactory.createConnection(configuration)
//    val hbadmin = hbConn.getAdmin
//    // 判断当前表是否被使用
//    if(!hbadmin.tableExists(TableName.valueOf(HbaseTableName))){
//      println("当前表可用")
//      // 创建表对象
//      val tableDescriptor = new HTableDescriptor(TableName.valueOf(HbaseTableName))
//      // 创建列簇
//      val hColumnDescriptor = new HColumnDescriptor("tags")
//      // 将创建好的列簇加入表中
//      tableDescriptor.addFamily(hColumnDescriptor)
//      hbadmin.createTable(tableDescriptor)
//      hbadmin.close()
//      hbConn.close()
//    }
//    val conf = new JobConf(configuration)
//    // 指定输出类型
//    conf.setOutputFormat(classOf[TableOutputFormat])
//    // 指定输出哪张表
//    conf.set(TableOutputFormat.OUTPUT_TABLE,HbaseTableName)

    // 读取数据文件
    val df = spark.read.parquet("D:\\outputData\\Log2parquet")

    // 读取字典文件
    val docsRDD = spark.sparkContext.textFile("src/main/data/app_dict.txt").map(_.split("\\s")).filter(_.length>=5)
      .map(arr=>(arr(4),arr(1))).collectAsMap()
    // 广播字典
    val broadValue = spark.sparkContext.broadcast(docsRDD)
    // 读取停用词典
    val stopwordsRDD = spark.sparkContext.textFile("src/main/data/stopwords.txt").map((_,0)).collectAsMap()
    // 广播字典
    val broadValues = spark.sparkContext.broadcast(stopwordsRDD)

    val allUserId = df.rdd.map(row=>{    //先映射（map），再拍扁
      // 获取所有用户拥有的唯一组合ID，List集合保存
      val strList = TagUtils.getallUserId(row)
      (strList,row)
    })
    // 构建点集合
    val verties = allUserId.flatMap(row=>{
      // 获取原始数据每一行的所有数据
      val rows = row._2

      //广告标签
      val adList = AdTag.makeTags(rows)
      //商圈标签
      val businessList = BusinessTag.makeTags(rows)
      // 媒体标签
      val appList = AppTag.makeTags(rows,broadValue)
      // 设备标签
      val devList = DeviceTag.makeTags(rows)
      // 地域标签
      val locList = LocationTag.makeTags(rows)
      // 关键字标签
      val kwList = KwordTag.makeTags(rows,broadValues)
      // 获取所有的标签
      val tagList = adList++ appList++devList++locList++kwList

      val VD = row._1.map((_,0))++tagList//每个用户拥有的所有唯一标识ID变成list((ID1,0)，(ID2,0))++taglist

      //思考 1. 如何保证其中一个ID携带着用户的标签
      //     2. 用户ID的字符串如何处理

      row._1.map(uId=>{
        //row._1.head指原数据每个用户所有ID集合strList的第一个ID,uId指其中的所有的ID，进行一一匹配
        //row._1.head不变，uId可变
        if(row._1.head.equals(uId)){
          (uId.hashCode.toLong,VD)//保证用户拥有的第一个ID一定拥有一定携带所有标签，因为点集合key数据设为了Long型，所以需要强转,自己测试不转不报错
        }else{
          (uId.hashCode.toLong,List.empty)//也要保证其它ID一定不拥有标签，但又要保证ID要存在，后面可能要用到
        }
      })
    })
    // 打印点集合测试
    //verties.take(20).foreach(println)
//    // 构建边的集合
    val edges = allUserId.flatMap(row=>{
      // A B C: A->B  A ->C   A不变，B，C可变，与测试例的边位置没什么关系，前后无所谓
      row._1.map(uId=>Edge(row._1.head.hashCode.toLong,uId.hashCode.toLong,0))
    })
    //打印边集合测试
    //edges.foreach(println)
    // 构建图
    val graph = Graph(verties,edges)
    // 根据图计算中的连通图算法，通过图中的分支，连通所有的点
    // 然后在根据所有点，找到内部最小的点，为当前的公共点
    val vertices = graph.connectedComponents().vertices


    println(vertices.collect().toBuffer)//打印顶点
   //println(vertices.join(verties).collect().toBuffer)//打印连接后的结果

    // 聚合所有的标签
    vertices.join(verties).map{
      case (uid,(cnId,tagsAndUserId))=>{
        (cnId,tagsAndUserId)
      }
    }.reduceByKey( //value值每个元素是list
      (list1,list2)=>{
      (list1++list2)
        .groupBy(_._1)//对value聚合成一个的list继续分组聚合，求出每个用户
        .mapValues(_.map(_._2).sum)
        .toList
    }).foreach(println)
      //map{//这里是存储到hbase,也可以直接打印，在这里接foreach(println())
//      case (userId,userTags) =>{
//        // 设置rowkey和列、列名
//        val put = new Put(Bytes.toBytes(userId))
//        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(2019924),Bytes.toBytes(userTags.mkString(",")))
//        (new ImmutableBytesWritable(),put)
//      }
//    }.saveAsHadoopDataset(conf)

   spark.stop()
  }
}


//结果1如下
//(-1100914919,List((AD27fd0fde315c7f45,0), (IM03de29984baffb87a7338d0ead22c045,0), (LC12,1), (CN100018,1), (APP爱奇艺,1), (D00010001,1), (D00020001,1), (D00030004,1), (ZP广东省,1), (ZC茂名市,1)))
//(1734240740,List())
//(-1820156136,List((ADe47c4b6890a3d56f,0), (IMeb8c8e1cce6a166a7f36752ef4e65756,0), (LC09,1), (CN100018,1), (APP爱奇艺,1), (D00010001,1), (D00020001,1), (D00030004,1), (ZP浙江省,1), (ZC台州市,1), (K言情剧,1), (K农村剧,1), (K内地剧场,1), (K华语剧场,1)))
//(-1686441139,List())

//结果2如下  //按每个连通图最小的顶点作为公共点相连，对照GraphTest
//Edge(-1100914919,-1100914919,0)
//Edge(-1100914919,1734240740,0)
//Edge(-1820156136,-1820156136,0)
//Edge(-1820156136,-1686441139,0)

//结果3如下
//ArrayBuffer((784323093,-1661689808), (-894413088,-894413088), (-643298446,-643298446),
//(-71660782,-786652588), (-2029373557,-2029373557), (38577520,-1660832626), (-1487563983,-1487563983),
//(-1545349295,-1545349295), (1359300697,1359300697), (1475458208,-356591069), (-389699738,-824206630),
//(-1158701430,-1158701430), (24898169,-37207533), (52563027,-1743798748), (1646161113,-476114765),


//结果4如下
//ArrayBuffer((784323093,(-1661689808,List((AD95c6c9346677d891,0), (IMA1000052C9F571,0),
//(LC12,1), (CN100018,1), (APP爱奇艺,1), (D00010001,1), (D00020001,1), (D00030004,1),
//(ZP江苏省,1), (ZC未知,1), (K中成本,1), (K热门电影,1), (K喜剧片,1), (K地点场景,1),
//(K动作片,1), (K配音语,1)))), (-894413088,(-894413088,List())), (-643298446,
//(-643298446,List())), (-71660782,(-786652588,List())), (-2029373557,(-2029373557,List((ADfcc30ccc9fb9ce35,0),
//(IMeeebf871095261c5dcfbd251630540eb,0), (LC12,1), (CN100018,1), (APP爱奇艺,1), (D00010001,1), (D00020001,1),
//(D00030004,1), (ZP浙江省,1), (ZC温州市,1), (K小帅哥,1), (K小帅哥,1)))), (38577520,(-1660832626,List())),
//(-1487563983,(-1487563983,List())), (-1545349295,(-1545349295,List((AD2d38876cb21407c4,0),

//结果5如下：
//(-1903998232,List((K游戏世界,1), (ZC西安市,1), (APP爱奇艺,1), (IMaecb2d65c118304bb7bf440a0612278e,0), (CN100018,1), (K经典游戏,1), (D00030004,1), (K单机游戏,1), (ZP陕西省,1), (D00010001,1), (ADec52c27bff57d1ef,0), (LC12,1), (D00020001,1)))
//(-894413088,List((ZP江苏省,1), (AD7b6e26a2828b8d39,0), (K游戏世界,1), (K网络游戏,1), (APP爱奇艺,1), (IM79bd4ba3546fc3054346b7b85e97b8bf,0), (LC09,1), (ZC南通市,1), (CN100018,1), (K经典游戏,1), (D00030004,1), (K单机游戏,1), (D00010001,1), (D00020001,1)))
//(-2004320458,List((AD483b1f3ff156cc7c,0), (APP爱奇艺,1), (ZC宁波市,1), (IM2b009d074332438c6b90d4f7b473384c,0), (ZP浙江省,1), (CN100018,1), (D00030004,1), (K女流氓,2), (D00010001,1), (LC12,1), (D00020001,1)))