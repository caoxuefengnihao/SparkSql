package cn.itheima.Sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object hive {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("kk").master("spark://dshuju02:7077").getOrCreate()
    val frame: DataFrame = spark.read.parquet("hdfs://dshuju01:9000/user/hive/warehouse/sc1")
    frame.createTempView("sc")
    val frame1: DataFrame = spark.sql("select * from sc")
    frame1.show()

  }

}
