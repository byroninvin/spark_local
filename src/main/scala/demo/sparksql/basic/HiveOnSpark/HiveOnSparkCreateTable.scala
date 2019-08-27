package demo.sparksql.basic.HiveOnSpark

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Joe.Kwan on 2018/8/24. 
  */
object HiveOnSparkCreateTable {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("hive_on_spark_create_table")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()


    val sql: DataFrame = spark.sql("CREATE TABLE niu_tesk (id bigint, name string)")
    sql.show()
    spark.stop()

  }

}
