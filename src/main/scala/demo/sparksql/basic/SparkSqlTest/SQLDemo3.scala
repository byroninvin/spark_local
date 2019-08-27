package demo.sparksql.basic.SparkSqlTest

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Joe.Kwan on 2018/8/22. 
  */
object SQLDemo3 {
  def main(args: Array[String]): Unit = {
    // 提交的这个程序可以连接到Spark集群中

    val conf = new SparkConf().setAppName("SQLDemo1").setMaster("local[*]")

    // 创建SparkSQL的连接,相当于程序执行的入口

    val sc = new SparkContext(conf)
    // SparkContext不能创建特殊的RDD(DataFrame)
    // 将SparkContext包装增减
    val sqlContext = new SQLContext(sc)
    // 创建特殊的RDD(DataFrame),就是有schema信息的RDD

    // 先有一个普通的RDD,然后再关联上schema信息,进而转换成DataFrame

    val lines = sc.textFile("hdfs://192.168.9.11:9000/person")
    // 将数据进行整理
    val rowRDD: RDD[Row] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toDouble
      Row(id, name, age, fv)
    })

    // 方法2:
    // 需要传入一个schema信息,结构类型,就是表头,用于描述DataFrame
    val schema = StructType(List(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("fv", DoubleType, true)
    ))

    // 将rowRDD关联schema
    val bdf: DataFrame = sqlContext.createDataFrame(rowRDD,schema)

    // 不适用SQL方式,就不用注册临时表了
    val df1: DataFrame = bdf.select("name", "age", "fv")


    import sqlContext.implicits._

    // $告诉它你以后把它当成一列, desc对应的一个方法,表达式. 不能大写
    val df2: Dataset[Row] = df1.orderBy($"fv" desc, $"age" asc)
    // sql api使用的可能更多

    df2.show()

    sc.stop()
  }

}
