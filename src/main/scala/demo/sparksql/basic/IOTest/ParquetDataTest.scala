package demo.sparksql.basic.IOTest

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Joe.Kwan on 2018/8/22. 
  */
object ParquetDataTest {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("parquetDataTest")
      .master("local[*]")
      .getOrCreate()

    // 读取parquet数据

    val df: DataFrame = spark.read.parquet("F:\\projectJ\\data\\test4\\part-00000-abb78ff2-2ee3-4442-be19-5ecfc6f4f536-c000.snappy.parquet")

    // 另外一种读取方法

    val df2: DataFrame = spark.read.format("parquet").load("F:\\projectJ\\data\\test4\\part-00000-abb78ff2-2ee3-4442-be19-5ecfc6f4f536-c000.snappy.parquet")

    // show是action

//    df.printSchema()

    // 很好,这种方法更智能,
    df2.show()
  }

}
