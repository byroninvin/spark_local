package demo.sparksql.basic.SparkSqlTest

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Joe.Kwan on 2018/8/28. 
  */
object SparkSQLDemo {

  def main(args: Array[String]): Unit = {


    // 构造方法
    val spark: SparkSession = SparkSession
      .builder() // 用到了java里面的构造器设计模式
      .appName("spark_sql_demo")
      .master("local[*]")
      // 这是Spark SQL 2.0里面重要的变化, 需要设置spark.sql的元数据仓库目录
      .config("spark.sql.warehouse.dir", "F:\\projectJ\\SparkTest\\spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    // 读取json文件,构造一个untyped类型的dataframe
    // 就相当与Data
    val df: DataFrame = spark.read.json("D:\\spark-2.2.1-bin-hadoop2.7\\examples\\src\\main\\resources\\people.json")
    df.show() // 打印数据
    df.printSchema() // 打印原数据
    df.select("name").show() // select操作,典型的弱类型操作
    df.select($"name", $"age" + 1).show()  // 使用表达式,scala语法, 要用$符号作为前缀
    df.filter($"age" > 21).show()  // filter操作加表达式的一个应用操作
    df.groupBy("age").count().show()   // 分组再聚合

    // 基于dataframe创建临时视图(表)
    df.createOrReplaceTempView("people")

    // 进行sql语句,得到的也是df格式,默认是针对操作临时视图的
    val sqlDF: DataFrame = spark.sql("SELECT * FROM people")
    sqlDF.show()



    // Dataset,强类型
    // 首先需要定义一个case class, 通常都需要定义一个cass class,自定义


    spark.stop()

  }

}
