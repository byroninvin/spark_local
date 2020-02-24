package demo.sparksql

import org.apache.spark.sql.SparkSession
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.apache.spark.ml.linalg.{Vector =>SparkVector}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.DenseVector

/**
 * Created by Joe.Kwan on 2019-11-1 15:22. 
 */
object SparkVectorBreeze {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()


    def toBreeze(v: SparkVector) = BV(v.toArray)
    def fromBreeze(bv:BV[Double]) = Vectors.dense(bv.toArray)

    val v1 = Vectors.dense(1.0, 2.0, 3.0)
    val v2 = Vectors.dense(4.0, 5.0, 6.0)
    val bv1 = new DenseVector(v1.toArray)
    val bv2 = new DenseVector(v2.toArray)

    // 测试点积
    val testDot = toBreeze(bv1) dot toBreeze(bv2)

    // 测试Norm
    val testNorm = Vectors.norm(bv1, 2)

    println(testDot,testNorm)


    /**
     * 函数
     * @param v1
     * @param v2
     * @return
     */
    def cosvec(v1: DenseVector, v2: DenseVector) = {
      val innerProduct= toBreeze(bv1) dot toBreeze(bv2)
      val cos = innerProduct / (Vectors.norm(bv1, 2) * Vectors.norm(bv2, 2))
      if (cos <=1) cos else 1.0
    }



    // 测试
    val testMytest = cosvec(bv1, bv2)
    println(testMytest)


    spark.stop()
  }

}
