package demo.sparksql.basic.IOTest

import demo.sparksql.basic.ConfigTest
import demo.sparksql.basic.IOTest.SaveAsCSV.getMysalProperty
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StructField}
import org.apache.spark.sql.{DataFrame, Row, types}

/**
  * Created by Joe.Kwan on 2018/8/25.
  */
object LoadFromCSV extends ConfigTest{

  val spark = buildSparkSession("loac_from_file")

  /**
    * 读取本地csv文件
    */
  def readCSV={
    val df1 = spark.read.option("header", true).csv("F:\\projectJ\\data\\csvtest")
      .select("side","area")

    df1.printSchema()

    // df1.collect().foreach(row => println(row.getString(0)+ "::" + row.getString(1)))
    // df1.createOrReplaceTempView("df1_view")
    // spark.sql("SELECT * FROM df1_view WHERE side >=90").collect().foreach(row=>println(row.getString(0)+ "::" + row.getString(1)))

    df1.toDF("side", "area").show()

    // 创建视图表再进行后续操作
    df1.createOrReplaceTempView("df1_view")

    val df2: DataFrame = spark.sql("SELECT * FROM df1_view WHERE side >=90")
    df2.show()
  }

  /**
    * 读取本地Json,并创建临时的视图,通用的js的对象,不需要指定option-header的信息,自动保存了schema信息
    * @param args
    */
  def readJson = {
    val df1 = spark.read.json("F:\\projectJ\\data\\jsontest")

    df1.printSchema()
    df1.toDF("side", "area").show()
    df1.createOrReplaceTempView("t")
    // spark.sql("select * from t where area< 200").collect().foreach(row=> println(row(0) + ":::" + row(1)))

    val df2: DataFrame = spark.sql("SELECT * FROM t WHERE area<200")
    df2.show()
  }

  /**
    * 读取RDD,本地虚拟机
    */
  def readRDD = {
    val df1 = spark.sparkContext.textFile("F:\\projectJ\\data\\texttest")

    val df2: RDD[Row] = df1.map(line => {
      val fields = line.split("::")
      val side = fields(0).toLong
      val area = fields(1).toLong
      Row(side, area)
    })

    val my_schema = types.StructType(List(
      StructField("side", LongType,true),
      StructField("area", LongType,true)
    ))

    val df3: DataFrame = spark.sqlContext.createDataFrame(df2,my_schema)

    // 将rowRDD关联schema
    df3.show()
  }


  /**
    * 从parquet文件中读取数据
    */

  def readParquet = {

    val df1: DataFrame = spark.read.parquet("F:\\projectJ\\data\\parquettest").toDF()
    df1.printSchema()

    df1.createOrReplaceTempView("t")

    val df2: DataFrame = spark.sql("select *from t where side =90")

    df2.show()
  }


  /**
    * 从关系型数据库中读取数据
    */

  def readFormMySql={

    val df1: DataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/test","df_saveToMySql_test",getMysalProperty)
    df1.printSchema()

    df1.createOrReplaceTempView("t")

    val df2: DataFrame = spark.sql("select * from t where area <100")

    df2.show()
  }


  /**
    * 主程序
    * @param args
    */
  def main(args: Array[String]): Unit = {

    readFormMySql
    spark.stop()
  }
}

