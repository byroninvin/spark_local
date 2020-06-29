package SparkRowMatirx

import org.apache.spark.mllib.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable

/**
 * Created by Joe.Kwan on 2020/6/8
 */
object SparkMultiplyRowMatrix {

  /**
   * Utility Methods to Transpose a org.apache.spark.mllib.linalg.distributed.RowMatirx
   * @param row
   * @param rowIndex
   * @return
   */
  def rowToTransposedTriplet(row: Vector, rowIndex: Long): Array[(Long, (Long, Double))] = {
    val indexedRow = row.toArray.zipWithIndex
    indexedRow.map{case (value, colIndex) => (colIndex.toLong, (rowIndex, value))}
  }

  def buildRow(rowWithIndexes: Iterable[(Long, Double)]): Vector = {
    val resArr = new Array[Double](rowWithIndexes.size)
    rowWithIndexes.foreach{case (index, value) =>
      resArr(index.toInt) = value
    }
    Vectors.dense(resArr)
  }

  def transposeRowMatrix(m: RowMatrix): RowMatrix = {
    val transposedRowsRDD = m.rows.zipWithIndex.map{case (row, rowIndex) => rowToTransposedTriplet(row, rowIndex)}
      .flatMap(x => x) // now we have triplets (newRowIndex, (newColIndex, value))
      .groupByKey
      .sortByKey().map(_._2) // sort rows and remove row indexes
      .map(buildRow) // restore order of elements in each row and remove column indexes
    new RowMatrix(transposedRowsRDD)
  }


  /**
   *
   * @param m1
   * @param m2
   * @param ctx
   * @return
   */
  def multiplyRowMatrices(m1: RowMatrix, m2: RowMatrix)(implicit ctx: SparkSession): RowMatrix = {

    // Zip m1 columns with m2 rows
    val m1Cm2R = transposeRowMatrix(m1).rows.zip(m2.rows)

    // Apply scalar product between each entry in m1 vector with m2 row
    val scalar = m1Cm2R.map{
      case(column:DenseVector,row:DenseVector) => column.toArray.map{
        columnValue => row.toArray.map{
          rowValue => columnValue*rowValue
        }
      }
    }

    // Add all the resulting matrices point wisely
    val sum = scalar.reduce{
      case(matrix1,matrix2) => matrix1.zip(matrix2).map{
        case(array1,array2)=> array1.zip(array2).map{
          case(value1,value2)=> value1+value2
        }
      }
    }

    new RowMatrix(ctx.sparkContext.parallelize(sum.map(array=> Vectors.dense(array))))
  }


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .enableHiveSupport()
      .getOrCreate()
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    // 设置orc解析模式 如果分区下没有文件也能在sql也能查询不会抛错
    spark.sqlContext.setConf("spark.sql.hive.convertMetastoreOrc", "false")

    import spark.implicits._

//    val mac_embedding = spark.read.parquet("/user/guanyue/recommendation/feature/vs_lfm_embedding/vs_lfm_mac_embedding.parquet").limit(100)

    val vid_embedding = spark.read.parquet("/user/guanyue/recommendation/feature/vs_lfm_embedding/vs_lfm_vid_embedding.parquet").limit(20)


    vid_embedding.show()



    val rdd_vid = vid_embedding.rdd.repartition(20).map { row =>
      val embedding_ori = row.getAs[mutable.WrappedArray[Double]]("vid_embedding").toArray
      Vectors.dense(embedding_ori)
    }

    rdd_vid.foreach(println)

//    val matrix_vid: RowMatrix = new RowMatrix(rdd_vid)
//
//
//    val result_matrix = multiplyRowMatrices(matrix_vid, matrix_vid)(spark)
//
//    result_matrix.rows.foreach(println)
//
//    spark.stop()
  }
}
