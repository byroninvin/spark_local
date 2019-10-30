package coocaa.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
  * Created by Joe.Kwan on 2019-10-10 14:55. 
  */
object SparkUtilsGetTimeStamp {

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
      * 测试currentTimeMillis   time的生成
      */

    val time1: Long = System.currentTimeMillis()

    println(time1)


    def takeSample(a:Array[String], n:Int) = {
      val rnd = new Random()
      Array.fill(n)(a(rnd.nextInt(a.size)))
    }


    val testArray = Array("a", "b", "c", "d", "e", "f")

    takeSample(testArray, 3).foreach(println)
    takeSample(testArray, 3).foreach(println)
    takeSample(testArray, 3).foreach(println)
    takeSample(testArray, 3).foreach(println)





    spark.stop()
  }

}
