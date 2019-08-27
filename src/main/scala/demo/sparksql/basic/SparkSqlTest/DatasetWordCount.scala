package demo.sparksql.basic.SparkSqlTest

import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * Created by Joe.Kwan on 2018/8/22. 
  */
object DatasetWordCount {

  def main(args: Array[String]): Unit = {

    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("SQLWordCount")
      .master("local[*]")
      .getOrCreate()

    // (指定以后从哪里)读数据,是Lazy
    // spark.read
    // Dataset也是一个分布式数据集,是一个更聪明的RDD,是对RDD的进一步封装,是更加智能的RDD
    // Dataset只有一列,默认这列叫value

    val lines: Dataset[String] = spark.read.textFile("hdfs://192.168.9.11:9000/wordcount/input")

    // 整理数据(切分压平), flatMap是RDD上的方法,需要导入隐式转换
    import spark.implicits._
    val words: Dataset[String] = lines.flatMap(_.split(" "))

    // 使用Dataset API(DSL)特定领域语言


    // 隐晦的方法
    // 导入聚合函数

    import org.apache.spark.sql.functions._
    // 一样可以
    //val result: Dataset[Row] = words.groupBy($"value" as("word")).agg(count("*") as "counts").orderBy($"counts" desc)

    val result: Dataset[Row] = words.groupBy($"value" as "word").agg(count("*") as "counts").orderBy($"counts" desc)


    result.show()

    spark.stop()
  }

}
