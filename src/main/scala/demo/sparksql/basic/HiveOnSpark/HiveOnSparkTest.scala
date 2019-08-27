package demo.sparksql.basic.HiveOnSpark

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Joe.Kwan on 2018/8/24. 
  */
object HiveOnSparkTest {

  def main(args: Array[String]): Unit = {


    // 一定要开启spark对hive的支持
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("hive_on_spark_test")
      .enableHiveSupport()  //启用spark对hive的支持(可以兼容hive的语法)
      .getOrCreate()


    // 想要使用hive的元数据库,必须指定hive元数据库的位置,如何指定,添加一个hive-site.xml到当前程序的classpath下即可


    // 有t_boy这个表或者视图吗?

    val result: DataFrame = spark.sql("SELECT * FROM big24.t_movie")


    result.show()

    spark.stop()




  }

}
