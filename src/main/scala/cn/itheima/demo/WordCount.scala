package cn.itheima.demo
import java.net.URL
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import scala.collection.mutable

/**
  *（1）groupByKey()是对RDD中的所有数据做shuffle,根据不同的Key映射到不同的partition中再进行aggregate。
  *（2）aggregateByKey()是先对每个partition中的数据根据不同的Key进行aggregate，然后将结果进行shuffle，完成各个partition之间的aggregate。因此，和groupByKey()相比，运算量小了很多。
  * (3)  distinct()也是对RDD中的所有数据做shuffle进行aggregate后再去重
  *（4）reduceByKey()也是先在单台机器中计算，再将结果进行shuffle，减小运算量
  * 所以在计算过程当中我们最好采用aggregateByKey() reduceByKey()等 Rdd 增加程序性能
  *
  *
  * 1. groupByKey某些情况下可以被reducebykey代替。
  * 2. reduceByKey某些情况下可以被 aggregatebykey代替。
  * 3. flatMap-join-groupBy某些情况下可以被cgroup代
  *
  *
  * //此算子是用来进行抽样调查的 进而判断出发生数据倾斜的Key
    val value: RDD[(String, Int)] = unit2.sample(false,0.1)
    //该函数返回的是一个map
    val stringToLong: collection.Map[String, Long] = value.countByKey()
    //遍历map 然后获取我们抽取的数据信息
    stringToLong.foreach(println(_))
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCount")
    conf.setMaster("local[*]")
    val context = new SparkContext(conf)
    val unit: RDD[String] = context.textFile("file:///C:\\Users\\Administrator\\Desktop\\wc.txt")
    val unit1 :RDD[String]= unit.flatMap((x:String) =>{x.split(" ")})
    val unit2 = unit1.map((x:String) => {(x,1)})
   /* //此算子是用来进行抽样调查的 进而判断出发生数据倾斜的Key
    val value: RDD[(String, Int)] = unit2.sample(false,0.1)
    //该函数返回的是一个map
    val stringToLong: collection.Map[String, Long] = value.countByKey()
    //遍历map 然后获取我们抽取的数据信息
    stringToLong.foreach(println(_))*/
    //
     val unit4 = unit2.reduceByKey(_+_)
    /*val unit4: RDD[(String, Iterable[Int])] = unit2.groupByKey()
      unit4.foreachPartition(it =>{it.map(line =>{(line._1,line._2.sum)})})*/
    unit4.saveAsTextFile("file:///C:\\Users\\Administrator\\Desktop\\wc")

  }
}
/**
  *
  * 案例二  统计每个学科的老师排名
  */
object subject{
  def main(args: Array[String]): Unit = {
   val conf = new  SparkConf
    conf.setAppName("favorite")
    conf.setMaster("spark://dshuju02:7077")
    val context = new SparkContext(conf)
   val k =  context.textFile("").map(line => {
     val url = new URL(line)
      //获得域名
      val host = url.getHost
      // 获得学科
      val sb = host.substring(0,host.indexOf("."))
      //获得老师
      val teacher = url.getPath.substring(1)
      //把学科和老师放到一个元组里面
      (sb,teacher)
    }).map(x =>{ (x,1)}).reduceByKey((x:Int,y:Int)=>{x+y}).groupBy(_._1._1)/*.mapValues(x=>x.toList.sortBy(y => y._2).reverse.take(3))*/
  }
}
/**
  * 案例三  通过自定义分区的方式实现统计每个学科的老师排名
  * 但是我们并不知道一共有多少个学科
  *
  */
object subject01{
  def main(args: Array[String]): Unit = {
    val conf = new  SparkConf
    conf.setAppName("favorite")
    conf.setMaster("spark://dshuju02:7077")
    val context = new SparkContext(conf)
    SparkContext.getOrCreate(conf)
    val value: RDD[((String, String), Int)] = context.textFile("hdfs://dshuju01:9000/subject.txt").map(line => {
      val url = new URL(line);
      //获得域名
      val host = url.getHost
      // 获得学科
      val sb = host.substring(0, host.indexOf("."))
      //获得老师
      val teacher = url.getPath.substring(1)
      //把学科和老师放到一个元组里面
      (sb, teacher)
    }).map(x => (x, 1)).reduceByKey((x, y) => {
      x + y
    })
    /**
      * reduceByKey()这个方法其实可以直接在传给他一个分区器 但是这个分区器一定是已经确定的
      * 具体请看案例四
      */
    /**
      * 在这里可以进行优化  我们通过分析发现
      * value: RDD[((String, String), Int)] 这个产生的数据
      * 可以缓存到内存中反复使用
      */
    value.cache()
    value.checkpoint()

    // 到这步为止 我们能够获得有多少个学科 用来做自定义分区
    val strings: Array[String] = value.map(x => {
      x._1._1
    }).distinct().collect()
    /** 在这里我们就可以定义自定义分区器了
      * 在这里 我们要对数据进行一下处理
      * 因为上一个结果返回来的类型是一个RDD[((String, String), Int)]
      * 而我们自定义分区需要传进去一个字符串我们将其改变为
      * Array[(String, (String, Int))]这种类型
      */
    val subjectpart = new subjectPartition(strings)
    val value1: RDD[(String, (String, Int))] = value.map(x => {
      (x._1._1, (x._1._2, x._2))
    }).partitionBy(subjectpart).mapPartitions( _.toList.sortBy(_._2._2).reverse.take(3).iterator)
    value1.saveAsTextFile("hdfs://dshuju01:9000/SubjectResult.txt")
    context.stop()
  }
}
/**
  *
  * 与MapReduce一样想要自定义分区 我们首先要继承 Partition 的类
  * 实现两个方法
  */
class subjectPartition(subjects:Array[String]) extends Partitioner{
  /**
    *
    * 自定义分区规则
    */
  val rules = new mutable.HashMap[String,Int]()
  var i = 1
  for (elem <- subjects) {
    rules += (elem -> i)
    i+= 1
  }
  /**
    *
    * 这个方法是有几个分区
    * @return
    */
  override def numPartitions: Int =  subjects.length + 1
  /**
    *
    * 这个方法是根据传入的key 决定该条数据到哪个分区
    * @param key
    * @return
    */
  override def getPartition(key: Any): Int ={
    rules.getOrElse(key.toString,0)
  }
}
/**
  * 案例四  通过自定义分区的方式实现统计每个学科的老师排名
  * 但是我们已经知道了一共有几个学科
  */
object subject02{
  def main(args: Array[String]): Unit = {
    val list = List("bigdata","java","php")
    val conf = new  SparkConf
    conf.setAppName("favorite")
    conf.setMaster("spark://dshuju02:7077")
    val context = new SparkContext(conf)
    val subjectAndTeacher: RDD[(String, String)] = context.textFile("hdfs://dshuju01:9000/subject.txt").map(line => {
      val url = new URL(line);
      //获得域名
      val host = url.getHost
      // 获得学科
      val sb = host.substring(0, host.indexOf("."))
      //获得老师
      val teacher = url.getPath.substring(1)
      //把学科和老师放到一个元组里面
      (sb, teacher)
    })
    subjectAndTeacher.cache()
    /**
      * 在这里我们已经求出来了学科和老师那么我们就要考虑了
      * 我们已经知道了一共有多少个学科 我可不可以先进行过滤到一个分区中
      * 然后在进行计算呢
      *
      * 经过下面的步骤我们不用通过分区就可以进行区分
      */
    for (elem <- list) {
      val filtered: RDD[(String, String)] = subjectAndTeacher.filter(x =>elem.equals(x._1))
      val tuples: Array[((String, String), Int)] = filtered.map((_,1)).reduceByKey(_+_).sortBy(_._2,false).take(2)
      println(tuples.toBuffer)
    }
  }
}
/**
  *
  * 案例5 广播变量的使用
  * Broadcast variables allow the programmer to keep a read-only variable cached on each machine rather than shipping a copy of it with tasks.
  * They can be used, for example, to give every node a copy of a large input dataset in an efficient manner.
  * Spark also attempts to distribute broadcast variables using efficient broadcast algorithms to reduce communication cost.
  * Spark actions are executed through a set of stages,
  * separated by distributed “shuffle”  operations.
  * Spark automatically broadcasts the common data needed by tasks within each stage.
  * The data broadcasted this way is cached in serialized form and deserialized before running each task.
  * This means that explicitly creating broadcast variables is only useful when tasks across multiple stages need the same data or when caching the data in deserialized form is important.
  */
object broadcast{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("broadcast")
    conf.setMaster("spark://dshuju02:7077")
    val sc = new SparkContext(conf)
    /**
      *
      * 在这里 我们定义了一个变量  但是当我们启动task程序的时候 这个变量 比随着task复制影响性能
      * 这是我们就用到了广播变量的方法
      */
    val rules = Map("cn"->"中国","us"->"美国","jp"->"日本")
    /**
      * 定义广播变量 返回一个引用
      * task在用这个引用调用广播变量
      */
    val broadcasted: Broadcast[Map[String, String]] = sc.broadcast(rules)
    val value: RDD[String] = sc.textFile("hdfs://dshuju01:9000/....")
    value.map(x =>{
      val strings: Array[String] = x.split(",")
      val guojia= strings(0)
      val rules_guojia: Map[String, String] = broadcasted.value
      val str: String = rules_guojia.getOrElse(guojia,"其他")
      (str,strings(1))
    })
  }
}
/**
  * 小的综合案例  通过Ip求出归属地
  */
object IpLocation{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("IpLocation")
    conf.setMaster("spark://dshuju02:7077")
    val sc = new SparkContext(conf)
    // 我们要读两种文件 ip规则 与 要处理的文件
    val Lines: RDD[String] = sc.textFile("...Ip.log")
    val values = Lines.map(x => {
      val strings: Array[String] = x.split("[|]")
      (strings(2).toLong, strings(3).toLong, strings(6))
    })
    val tuples: Array[(Long, Long, String)] = values.collect()
   val broadcast: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(tuples)

    val access: RDD[String] = sc.textFile("access.log")
    val proyinceAndOne: RDD[(String, Int)] = access.map(x => {
      val strings: Array[String] = x.split("[|]")
      //将获得的ip 转化为 Long类型
      val l: Long = IpToLong(strings(1))
      //将获得的数据进行二分法查找
      val value: Array[(Long, Long, String)] = broadcast.value
      val i: Int = brnarySearch(value, l)
      (value(i)._3, 1)
    })
    val provinceAndCount: RDD[(String, Int)] = proyinceAndOne.reduceByKey(_+_)
    provinceAndCount.collect()
    sc.stop()
  }
  /**
    * 这个小工具方法为将ip转换为十进制的数
    * @param ip Ip地址
    * @return long 类型的十进制数
    */
  def IpToLong (ip:String): Long = {
    val fragments = ip.split("[.]")
    var iplong = 0L
    for (i <- 0 until fragments.length){
      iplong = fragments(i).toLong | iplong << 8L
    }
      iplong
  }
  /**
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
}