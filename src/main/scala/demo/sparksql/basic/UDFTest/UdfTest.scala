package demo.sparksql.basic.UDFTest

import demo.sparksql.basic.ConfigTest
import demo.sparksql.basic.IOTest.SaveAsCSV.getMysalProperty
import org.apache.spark.sql.DataFrame

/**
  * Created by Joe.Kwan on 2018/8/25. 
  */
object UdfTest extends ConfigTest{

  val spark = buildSparkSession("udf_test")

  def udf = {

    spark.udf.register("my_func1", (area:Int)=>{
      if(area >=3300) true else false
    })

  }

  def testudf ={
    val df1: DataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/test","df_saveToMySql_test",getMysalProperty)


    df1.printSchema()
    df1.createOrReplaceTempView("t")

    spark.sql("select * from t where my_func1(area)").show()


  }

  def main(args: Array[String]): Unit = {
    udf
    testudf
    spark.stop()

  }

}
