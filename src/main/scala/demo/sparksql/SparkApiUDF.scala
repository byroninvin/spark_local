package demo.sparksql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

import scala.collection.mutable

/**
  * Created by Joe.Kwan on 2019-9-25 11:26. 
  */
object SparkApiUDF {

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
      * 方法一
      * 需要定义一个控制范围的函数
      * 将输出的结果运行n次 x = 10 * sqrt(x)
      */
    def shrinkGap(a: Double, n: Long):Double = {

      if (n < 1)
        a
      else
        10 * scala.math.sqrt(shrinkGap(a, n-1))
    }

    spark.udf.register("shrinkGap", shrinkGap _)


    /**
      * 场景二
      */

    val floatToDenseVector = (array: mutable.WrappedArray[Float]) => Vectors.dense(array.toArray.map(_.toDouble))
    val floatToDenseVectorUDF = udf(floatToDenseVector)


    /**
     * 场景三
     */
    //创建一个UDF判断需要取出的影片名, 下面要用到
    spark.udf.register("del_name", (title: String) => {

      var statu = 0
      val list_del = List("3D", "HDR", "Dolby Vision", "DRM", "4K", "Atmos")

      for (i <- list_del.indices) {
        if (title.contains(list_del(i))) {

          statu = 1
        }
      }

      statu
    })


    spark.stop()

  }

}
