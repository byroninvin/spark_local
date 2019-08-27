package demo.sparksql.window

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


/**
  * Created by Joe.Kwan on 2019-8-26 16:54. 
  */
object SparkApiWindowFunctions {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.rdd.compress", "true")
    sparkConf.set("hive.metastore.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
    sparkConf.set("spark.sql.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
    val spark = SparkSession.builder()
      .config(sparkConf)
      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    /**
      * 创造一份数据
      * ﻿{"name":"A","lesson":"Math","score":100}
      * {"name":"B","lesson":"Math","score":100}
      * {"name":"C","lesson":"Math","score":99}
      * {"name":"D","lesson":"Math","score":98}﻿
      * {"name":"A","lesson":"E","score":100}
      * {"name":"B","lesson":"E","score":99}
      * {"name":"C","lesson":"E","score":99}
      * {"name":"D","lesson":"E","score":98}
      */
    import spark.implicits._
    val testDF = Seq(
      ("A", "Math", 100),
      ("B", "Math", 100),
      ("C", "Math", 99),
      ("D", "Math", 98),
      ("A", "E", 100),
      ("B", "E", 99),
      ("C", "E", 99),
      ("D", "E", 99)
    ).toDF("name", "lesson", "score")
    testDF.createOrReplaceTempView("my_test_df")

    testDF.printSchema()

    /**
      * 使用spark.sql, sql语句方式完成
      */
    spark.sql(
      """
        |SELECT `name`, lesson, score,
        |   ntile(2) over(partition by lesson order by score desc) as ntile_2,
        |   ntile(3) over(partition by lesson order by score desc) as ntile_3,
        |   row_number() over(partition by lesson order by score desc) as row_number,
        |   rank() over(partition by lesson order by score desc) as rank,
        |   dense_rank() over(partition by lesson order by score desc) as dense_rank,
        |   percent_rank() over(partition by lesson order by score desc) as percent_rank
        |FROM my_test_df
        |order by lesson, name, score
      """.stripMargin).show()

    /**
      * 使用spark.sql, api的方式完成相同的功能
      */
    val window_spec = Window.partitionBy("lesson").orderBy(col("score").desc)    // 窗口函数中公用的子句

    testDF.select(
      col("name"), col("lesson"), col("score"),
      ntile(2).over(window_spec).as("ntile_2"),
      ntile(3).over(window_spec).as("ntile_3"),
      row_number().over(window_spec).as("row_number"),
      rank().over(window_spec).as("rank"),
      dense_rank().over(window_spec).as("dense_rank"),
      percent_rank().over(window_spec).as("percent_rank")
    ).orderBy("lesson", "name", "score").show


    spark.stop()

  }

}
