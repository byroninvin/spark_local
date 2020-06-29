import org.apache.spark.ml.linalg
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib
import org.apache.spark.mllib.linalg.{SparseVector => OldSparseVector, Vectors => OldVectors}
import com.github.fommil.netlib.{F2jBLAS, BLAS => NetlibBLAS}
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import util.{Breeze2SparkConverter, Spark2BreezeConverter, myPackage}
import breeze.linalg._
import breeze.numerics._

/**
 * Created by Joe.Kwan on 2020/5/29
 */
object SparkUserItemDistence {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.rdd.compress", "true")
      .config("spark.speculation.interval", "10000ms")
      .config("spark.sql.tungsten.enabled", "true")
      .config("spark.sql.shuffle.partitions", "800")
      .config("hive.metastore.uris", "thrift://xl.namenode2.coocaa.com:9083")
      .config("spark.sql.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()


    val test_sv01: linalg.Vector = Vectors.sparse(5, Array(1,4), Array(0.5, 0.95))
    val test_sv02: linalg.Vector = Vectors.sparse(5, Array(0,2), Array(0.5, 0.85))
    val test_sv03: linalg.Vector = Vectors.sparse(5, Array(1,4), Array(0.5, 0.5))
    val test_sv04: linalg.Vector = Vectors.sparse(5, Array(1,4), Array(0.5, 0.95))

    val result1: Double = Vectors.sqdist(test_sv01, test_sv02)
    val result2: Double = Vectors.sqdist(test_sv01, test_sv03)


    val test = util.OriBLAS.dot(test_sv01, test_sv04)

    val nor_sv01 = Vectors.norm(test_sv01, 2.0)
    val nor_sv04 = Vectors.norm(test_sv04, 2.0)

    val result3 = test / (nor_sv01 * nor_sv04)


    /**
     * 测试稀疏向量相加
     */




//    val test01: BSV[Double] = Spark2BreezeConverter.sparseSpark2SparseBreezeConverter.convert(test_sv01.toSparse)
//    val test02: BSV[Double] = Spark2BreezeConverter.sparseSpark2SparseBreezeConverter.convert(test_sv02.toSparse)

    val test01 = Spark2BreezeConverter.sparseSpark2SparseBreezeConverter.convert(test_sv01.toSparse)
    val test02 = Spark2BreezeConverter.sparseSpark2SparseBreezeConverter.convert(test_sv02.toSparse)


    val result = Breeze2SparkConverter.sparseBreeze2SparseSparkVector.convert(test01 * 0.5 + test02 * 0.5)
//    val test02 = Breeze2SparkConverter.sparseBreeze2SparseSparkVector.convert(test01)

    println(result)



//    println("result1:", result1)
//    println("result2:", result2)
//    println("result3:", result3)

    spark.stop()
  }


}
