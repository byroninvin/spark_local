package ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

import scala.collection.mutable

/**
  * Created by Joe.Kwan on 2019-8-26 16:54.
  * 关于ALS里面特征落地的一个坑
  *
  *
  */
object SparkMlArrayFloatToDenseVector {

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
      *
      */

//    val toDenseVector: Array[Float] => DenseVector = _.asInstanceOf[DenseVector]
//    val toDenseVector: Array[Float] => DenseVector = map(x => Vectors.dense(x))
//
//
//    val toDenseVector: DenseVector => Any = (array: Array[Float]) => Vectors.dense(array)

//    val toDenseVector = (array: Array[Double]) => Vectors.dense(array)
//    val toDenseVectorUdf = udf(toDenseVector)
//
//    val toArrayDouble = (array: Array[Float]) => array.map(_.toDouble)
//    val toArrayDoubleUdf = udf(toArrayDouble)

    /**
      * 方法一
      * @param array
      * @return
      */

    def toDenseVector(array: mutable.WrappedArray[Float]) = {

      Vectors.dense(array.toArray.map(_.toDouble))
    }

    spark.udf.register("toDenseVectorUDF", toDenseVector _)


    /**
      * * 方法二
      * * @param array
      * * @return
      */
    val floatToDenseVector = (array: mutable.WrappedArray[Float]) => {
      Vectors.dense(array.toArray.map(_.toDouble))
    }

    val floatToDenseVectorUDF = udf(floatToDenseVector)




    val testDF = spark.sql("""select * from test.album_itembasedcf_for_album""")


//    testDF.withColumn("vector", callUDF("toDenseVectorUDF", testDF.col("features"))).show()
    testDF.withColumn("vector", floatToDenseVectorUDF(col("features"))).show()

    /**
      * StructType(StructField(id,IntegerType,true), StructField(features,ArrayType(FloatType,true),true))
      */

//    import spark.implicits._
//
//    testDF.rdd.map(
//      row=> {
//        val id = row.getAs[Int]("id")
//        val features = row.getAs(1).asInstanceOf[mutable.WrappedArray[Float]]
//        val doubleFeatures = features.map(_.toDouble).toArray
//        (id, Vectors.dense(doubleFeatures))
//      }
//    ).toDF("id", "features").show(20)



    spark.stop()

  }

}
