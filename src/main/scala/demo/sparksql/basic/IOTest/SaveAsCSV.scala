package demo.sparksql.basic.IOTest

import java.util.Properties

import demo.sparksql.basic.ConfigTest

/**
  * Created by Joe.Kwan on 2018/8/24. 
  */
object SaveAsCSV extends ConfigTest{

  val spark = buildSparkSession("test_csv")


  def saveCsv() = {

    import spark.implicits._
    spark.sparkContext.makeRDD(1 to 100).map(row => (row, row*row)).toDF("side", "area")
      .write.option("header", true).csv("F:\\projectJ\\data\\csvtest")
//      csv("hdfs://master:9000//testfolder/csv")
  }

  def saveJson()={

    import spark.implicits._
    spark.sparkContext.parallelize(1 to 100).map(row => (row, row*row)).toDF("side", "area")
      .write.json("F:\\projectJ\\data\\jsontest")
  }

  def saveParquet() ={

    import spark.implicits._
    spark.sparkContext.parallelize(1 to 100).map(row =>(row, row*row)).toDF("side", "area")
      .write.parquet("F:\\projectJ\\data\\parquettest")
  }


  def saveToMySql() ={

    import spark.implicits._
    spark.sparkContext.parallelize(1 to 100).map(row => (row, row* row)).toDF("side","area")
      .write.jdbc("jdbc:mysql://localhost:3306/test","df_saveToMySql_test", getMysalProperty)
  }


  def saveToText() ={

    import spark.implicits._
    spark.sparkContext.makeRDD(1 to 100).map(row => (row+"::"+row*row)).toDF()
      .write.text("F:\\projectJ\\data\\texttest")
  }



  // 需要设置这个getMysalProperty
  def getMysalProperty: Properties = {
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "741852")
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop
  }

  def main(args: Array[String]): Unit = {

    saveToText()

    spark.stop()
  }

}
