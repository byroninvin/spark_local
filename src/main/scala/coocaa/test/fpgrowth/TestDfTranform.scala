package coocaa.test.fpgrowth

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by Joe.Kwan on 2018/12/11 13:50. 
  */
object TestDfTranform {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val spark: SparkSession = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import spark.implicits._

    val oriDf = spark.read.option("header", true).csv("F:\\projectDB\\Coocaa\\FpGrownth\\for_test_changed.csv").select("eq")


    oriDf.show()


    // 转换成rdd形式, 因为要使用MLLIB
    val transactions: RDD[Array[String]] = oriDf.rdd.map {
      s =>
        val str = s.toString().drop(1).dropRight(1)
        str.trim().split("[|]")
    }

    transactions.toDF().show()


    // df_week_lift_new = df_week_lift.withColumn('antecedent', concat_ws(',', 'antecedent')).withColumn('consequent', concat_ws(',', 'consequent'))









    spark.stop()


  }

}
