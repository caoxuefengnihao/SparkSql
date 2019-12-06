package cn.itheima.Sql
import java.sql.Connection

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{types, _}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}

import scala.collection.mutable
/**
  *
  * 1.x版本的SparkSql 的创建与使用
  */

object SqlContextdemo {
  def main(args: Array[String]): Unit = {
    val conf = new  SparkConf
    conf.setAppName("favorite")
    conf.setMaster("spark://dshuju02:7077")
    val context = new SparkContext(conf)
    val unit: RDD[String] = context.parallelize(List("1 zhagnsan 17 99","2 lisi 18 100","3 wangwu 19 100","4 zhaoliu 20 1000"))
    //将rdd 与 case class 关联在一起
    val unit2: RDD[Person] = unit.map(x => {
      val strings: Array[String] = x.split(" ")
      Person(strings(0), strings(1), strings(2).toInt, strings(3).toInt)
    })
    //将unit2转换成dataframe
    //创建sqlContext
    val spark = new SQLContext(context)
    import spark.implicits._
    //将RDD 转换成dataframe
    val frame: DataFrame = unit2.toDF()
    //将dataframe 注册一个 临时表
   frame.registerTempTable("person")
    //使用sql进行查询
    val frame1: DataFrame = spark.sql("select * from person  ")
    //触发action
    frame1.show()
    println(unit2.collect().toBuffer)
  }
}
case class Person (id:String,name:String,age:Int,fv:Int) extends Comparable[Person]{
  override def compareTo(o: Person): Int = {
    this.fv-o.fv
  }
}
/**
  * 用第二种方式来创建 dataframe
  */
object SqlContextdemo01{

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val values: RDD[String] = sc.textFile("hdfs://....")
    val personRow: RDD[Row] = values.map(x => {
      val strings: Array[String] = x.split(" ")
      Row(strings(0).toInt, strings(1), strings(2).toInt)
    })
    val screame = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    )
    val sql = new SQLContext(sc)
    val frame: DataFrame = sql.createDataFrame(personRow, screame)
    frame.registerTempTable("t_person")
    val sql1 = new SQLContext(sc)
    val frame1: DataFrame = sql1.sql("select * from t_person")
    frame1.show();
  }
}
/**
  * 使用spark2.x版本来进行编写程序  创建 dataframe
  * 之前的SparkContext 和 SparkConf 不在使用
  */
object HelloDatasets{
  def main(args: Array[String]): Unit = {
    //这个sparksession 不用new  而是用一个工厂来构建
    val session =  SparkSession.builder().appName("datasets").master(".....").getOrCreate()
    val screame = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    )
    /**
      * 此方法需要两个参数  第一个是RDD 第二个是 一个StructType 一个列的种类
      * 但是我们用了sparksession 来构建 并没有创建 SparkContext 但是我们可以通过
      * sparksession来获取SparkContext
      */
    val sc: SparkContext = session.sparkContext
    val values: RDD[String] = sc.textFile("hdfs://....")
    val personRow: RDD[Row] = values.map(x => {
      val strings: Array[String] = x.split(" ")
      Row(strings(0).toInt, strings(1), strings(2).toInt)
    })
    val frame: DataFrame = session.createDataFrame(personRow,screame)

    /**
      * 在1.x的版本中我们 通过registerTempTable 来注册临时表
      * 在2.x的版本中我们 通过createTempView 来注册临时表
      */
    frame.createTempView("v_person")

    /**
      * 在1.x的版本中我们 通过SQLContext来进行查询
      * 在2.x的版本中我们 通过session.sql的方法来进行查询
      */
    val frame1: DataFrame = session.sql("select * from v_person")
    frame1.show()
  }
}
/**
  *
  * 使用2.x的方式来创建 DataFrame 并且通过SQl方式来进行查询
  */
object DataFrameDemo{
  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder().appName("DataFrameDemo").master("spark://dshuju02:7077").getOrCreate()
    //指定读取数据的位置
    //val frame: DataFrame = session.read.format("text").text("hdfs://dshuju01:9000/wc.txt")
    val dataset: Dataset[String] = session.read.format("text").textFile("hdfs://dshuju01:9000/wc.txt")
    //但是会报错  我们需要导入SparkSession对象中的隐式转换
    import session.implicits._
    val unit: Dataset[String] = dataset.flatMap(_.split(" "))
    val frame: DataFrame = unit.withColumnRenamed("value","word")
    frame.createTempView("w_wc1")
    val frame1: DataFrame = session.sql("select word,count(*) from w_wc1 group by word")
    frame1.show()
  }
}
/**
  *
  * 使用2.x 的方式来创建dataframe 并且通过DataFrame API 的方式 来进行对数据的处理
  */
object DataFrameDemoAPI{
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("DataFrameDemoAPI").master("spark://dshuju02:7077").getOrCreate()
   val dataset: Dataset[String] = spark.read.textFile("hdfs://dshuju01:9000/wc.txt")
    import spark.implicits._
    val dataset01: Dataset[String] = dataset.flatMap(x=>x.split(" ")
    )
    val frame: DataFrame = dataset01.withColumnRenamed("value","word")
    frame.printSchema()
    /**
      * printSchema 打印Dataframe相关的schema信息
      * show的方法 可以指定展示出多少条信息 默认是20条
      * 但是在实际的工作当中 我们不能够show 我们应该把数据保存起来
      */
    frame.select(frame.col("word")).groupBy(frame.col("word")).count().show()
  }
  /**
    * 使用DataFrame 或 Sql 处理数据 首先 我们要将非结构化的数据 转变成结构化
    * 的数据 然后注册视图，执行sql(Transformation) 最后触发Action
    */
}
/**
  * 换一种加载方式 返回DataFrame 用Row 里的方法进行对数据的处理
  */
object DataFrameDemoAPI01{
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("DataFrameDemoAPI01").master("spark://dshuju02:7077").getOrCreate()
    val frame: DataFrame = spark.read.format("text").load("hdfs://dshuju01:9000/wc.txt")
    import spark.implicits._
    val dataset: Dataset[String] = frame.flatMap(r =>r.getAs[String]("value").split(" "))
    val frame1: DataFrame = dataset.withColumnRenamed("value","word")
    val frame2: DataFrame = frame1.select(frame.col("word")).groupBy(frame.col("word")).count()
    frame2.foreachPartition(part =>{
      RidesDemo(part)
    })

    /**
      * 但是我们现在不想show 想保存到redis 或者 MySql当中 那么我们该怎么办
      */
  }
  def RidesDemo(part:Iterator[Row]): Unit ={
    val connect :Connection = null
    for (elem <- part) {
    val str: String = elem.getAs[String](0)
    val i: Int = elem.getAs[Int](1)
    }
  }
}

/**
  * 通过自定义函数的方式 来 实现通过Ip求出归属地 的需求
  * 2.x de 方式
  * 并且通过join的方式
  *
  *
  * 但是这种方式执行的效率太慢了 我们怎么进行优化呢  我们要进行避免产生shuffle的过程
  * 因为shuffle的过程是非常消耗时间的
  */
object UDFDemo{


  val IptoLong = (ip:String) =>{val fragments = ip.split("[.]")
    var iplong = 0L
    for (i <- 0 until fragments.length){
      iplong = fragments(i).toLong | iplong << 8L
    }
    iplong}
  def main(args: Array[String]): Unit = {
    // 首先我们先将规则处理一下 并且转换成DataFrame 注册成临时表
    val session: SparkSession = SparkSession.builder().appName("UDFDemo").master("spark://dshuju02:7077").getOrCreate()
    val dataset: Dataset[String] = session.read.textFile("hdfs://dshuju01:9000/Iplogs.txt")
    import session.implicits._
    val unit: Dataset[(String, String, String)] = dataset.map(line => {
      val strings: Array[String] = line.split("[|]")
      (strings(2), strings(3), strings(6))
    })
    val frame: DataFrame = unit.toDF("startNum","endNum","province")
    frame.createTempView("IP-Rules")
    // 然后 我们在将access.logs 文件 处理一下 并将其转换成临时表
    val dataset01: Dataset[String] = session.read.textFile("hdfs://dshuju01:9000/access.logs")
    val unit01: Dataset[String] = dataset01.map(x => {
      val strings: Array[String] = x.split("[|]")
      strings(1)
    })
    val frame01: DataFrame = unit01.toDF("Ip")
    frame01.createTempView("IP-access")
    //自定义udf函数
    session.udf.register("IptoLong",IptoLong)
    //进行SQL查询
    val result: DataFrame = session.sql("select province,count(*) from  IP-access join   IP-Rules on (IptoLong(Ip)>=startNum And IptoLong(Ip)<= endNum) group by province")
    result.show()
   /* result.foreachPartition(p=>{
      p.foreach(r=>{
        r.

      })
    })*/
    session.stop()
  }
}

/**
  * 通过自定义函数的方式 来 实现通过Ip求出归属地 的需求
  * da
  *
  */

object UDFDemo01{
  /**
    *
    * 这个方法为二分法查找
    * @param line
    * @return
    */
  def brnarySearch(line :Array[(Long,Long,String)],ip:Long): Int ={
    var low = 0
    var hight = line.length-1
    while (low<=hight){
      var middle = (low+hight)/2
      if (ip>= line(middle)._1 && ip <= line(middle)._2){
        return middle
      }
      if(ip <= line(middle)._1){
        hight = middle-1
      }else{
        low = middle+1
      }
    }
    -1
  }
  def IpToLong (ip:String): Long = {
    val fragments = ip.split("[.]")
    var iplong = 0L
    for (i <- 0 until fragments.length){
      iplong = fragments(i).toLong | iplong << 8L
    }
    iplong
  }
  def main(args: Array[String]): Unit = {

    // 首先我们先将规则处理一下 并且转换成DataFrame 注册成临时表
    val session: SparkSession = SparkSession.builder().appName("UDFDemo").master("spark://dshuju02:7077").getOrCreate()
    val dataset: Dataset[String] = session.read.
      textFile("hdfs://dshuju01:9000/Iplogs.txt")
    import session.implicits._
    val unit: Dataset[(Long, Long, String)] = dataset.map(line => {
      val strings: Array[String] = line.split("[|]")
      (strings(2).toLong, strings(3).toLong, strings(6))
    })
    //为了避难shuffle 我们将已经处理好的Iprules 运用广播变量的方式广播出去
    val tuples: Array[(Long, Long, String)] = unit.collect()
    val unit1: Broadcast[Array[(Long, Long, String)]] = session.sparkContext.broadcast(tuples)

    // 然后 我们在将access.logs 文件 处理一下 并将其转换成临时表
    val dataset01: Dataset[String] = session.read.textFile("hdfs://dshuju01:9000/access.logs")
    val unit01: Dataset[String] = dataset01.map(x => {
      val strings: Array[String] = x.split("[|]")
      strings(1)
    })
    val frame01: DataFrame = unit01.toDF("ip")
    frame01.createTempView("IP-access")
    // 注册 自定义函数
    session.udf.register("IPToProvince",(Ip:String)=>{
      val l: Long = IpToLong(Ip)
      val value: Array[(Long, Long, String)] = unit1.value
      //获取ip所在规则的下标
      val i: Int = brnarySearch(value,l)
      value(i)._3
    })
    //编写sql 但是SQL当中并没有符合我们业务逻辑的函数 我们需要自己编写并注册
    val frame: DataFrame = session.sql("select IPToProvince(ip) province,count(*) from IP-access group by province")
    frame.write.format("json").save()
  }
}


/**
  *自定义UDAF 有些时候 需要个根据相关的业务逻辑 来自定义编写UDAF函数
  * 那么 我们怎么定义一个UDAF函数呢
  *
  * 例如我们想求出数字的几何平方根怎么办呢
  *
  */
class UserFunction extends UserDefinedAggregateFunction{

  /**
    * 第一个方法为：UDAF与DataFrame列相关的输入样式。StructField 的名字并没有特别的要求
    * 完全可以认为是两个内部结构的列名占位符
    * 至于UDAF具体要操作DataFrame的哪个列，取决于调用者，但前提是数据类型必须符合事先的设置，如这里的double
    * @return
    */
  override def inputSchema: StructType = StructType(
    List(
      StructField("value",DoubleType)
    )
  )

  /**
    * 定义存储聚合运算时产生的中间数据结果的schema
    * @return
    */
  override def bufferSchema: StructType =  StructType(
    List(
      StructField("count",LongType),
      StructField("product",DoubleType)
    )
  )

  /**
    * 这个方法表明了UDAF函数的返回值类型
    * @return
    */
  override def dataType: DataType = DoubleType

  /**
    * 用已标记针对给定的一组输入，UDAF是否总是生成相同的结果
    * @return
    */
  override def deterministic: Boolean = true

  /**
    *
    * 对聚合函数运算的中间结果的初始化
    * 这个以后用户自定义的
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 1.0
  }

  /**
    * 每处理一条数据 都要执行一下updata
    * 但是要注意 有多个分区 这个方法是执行在每个分区上的方法 相当于局部计算
    * @param buffer 就是自定义初始化的值
    * @param input 输入的每一行数据
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0)+1
    buffer(1) = buffer.getAs[Double](1)*input.getAs[Double](0)
  }

  /**
    * 这个方法是将局部的计算的结果合并成最终的结果
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0)+buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getAs[Double](1)*buffer2.getAs[Double](1)
  }

  /**
    * 执行计算逻辑
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Any = {
    math.pow(buffer.getDouble(1),1.0/buffer.getLong(0))
  }
}
object UserFunction{
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName("UserFunction").master("spark://dshuju02:7077").getOrCreate()
    val unit: Dataset[String] = session.read.textFile("hdfs://dshuju01:9000/math.txt")
    val frame: DataFrame = unit.toDF("math")
    frame.createTempView("v_math")
    val gm = new UserFunction
    //注册自定义函数
    session.udf.register("gm",gm)
    session.sql("select gm(math) as gm from v_math ").show()
  }


}

object DataSetYuan{
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("").master("").getOrCreate()
    val frame: DataFrame = session.read.format("jdbc").options(Map("url"->"")).load()

    //这个可以直接读取hive当中的表
    session.read.table("")
    frame.write.saveAsTable("")


  }

}
