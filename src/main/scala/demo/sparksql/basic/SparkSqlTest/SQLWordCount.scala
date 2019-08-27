package demo.sparksql.basic.SparkSqlTest

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by Joe.Kwan on 2018/8/22. 
  */
object SQLWordCount {

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

    // 注册视图
    words.createTempView("v_wc")

    // 执行SQL(Transformation,lazy)
    val result: DataFrame = spark.sql("SELECT value, COUNT(*) counts FROM v_wc GROUP BY value ORDER BY counts DESC")
    result.show()
    spark.stop()
  }

}
