package demo.sparksql.basic.IOTest

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Joe.Kwan on 2018/8/22. 
  */
object CsvDataSource {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("CsvDataTest")
      .master("local[*]")
      .getOrCreate()

    // 读取csv文件
    val csvData: DataFrame = spark.read.csv("F:\\projectJ\\data\\test3\\part-00000-cb1c1555-b974-4ef8-9472-a69c69aa791c-c000.csv")

    val csvDataWithColumns: DataFrame = csvData.toDF("id", "obj_type", "author_id")

    // schema信息都是当成了String类型, 相当于没有源数据信息
//    csvDataWithColumns.printSchema()
    csvDataWithColumns.show()

  }

}
