package demo.sparksql.basic.SparkSqlTest

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Created by Joe.Kwan on 2018/8/22. 
  */
object SQLTest1 {

  def main(args: Array[String]): Unit = {

    // Spark2.x SQL的编程API(SparkSession)

    // 是Spark2.x SQL执行入口
    val session: SparkSession = SparkSession.builder()
      .appName("SQLTest1")
      .master("local[*]")
      .getOrCreate()


    // 创建的前提是创建RDD
    // 创建RDD
    val lines: RDD[String] = session.sparkContext.textFile("hdfs://192.168.9.11:9000/person")
    // 整理数据
    val rowRDD: RDD[Row] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toDouble
      Row(id, name, age, fv)
    })

    // 生成schema
    val schema = StructType(List(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("fv", DoubleType, true)
    ))


    // 生成DataFrame
    val df: DataFrame = session.createDataFrame(rowRDD, schema)

    import session.implicits._

    val df2: Dataset[Row] = df.where($"fv" >98).orderBy($"fv" desc, $"age" asc)

    df2.show()

    session.stop()
  }

}
