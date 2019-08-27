package demo.sparksql.basic.IOTest

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Joe.Kwan on 2018/8/22. 
  */
object JsonDataSource {

  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder().appName("JdbcDataSource")
      .master("local[*]")
      .getOrCreate()

    val jsons: DataFrame = spark.read.json("F:\\projectJ\\data\\test2\\part-00000-7c874862-8fda-4cf3-ad35-4472f8fed7ba-c000.json")

    jsons.show()

    spark.stop()
  }

}
