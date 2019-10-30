package machinelearning

import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

/**
  * Created by Joe.Kwan on 2019-10-23 14:20. 
  */
object SparkDataFrameCalclateCosineSimilarity {

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
      * 创建vecotr
      */
    val tableA = spark.sql("""SELECT video_id, ori_vector FROM test.vs_video_features_from_als""")
      .select("video_id","ori_vector")

//    tableA.schemaindexMap
    import spark.implicits._



    val rddIndex = tableA.rdd.map (
      row => {
        val vid = row.getString(0)
        val vector = row.getAs[scala.collection.mutable.WrappedArray[Float]](1).map(_.toDouble)

        (vid, org.apache.spark.mllib.linalg.Vectors.dense(vector.toArray))
      }
    ).zipWithIndex()

    val indexMap = rddIndex.map{case ((id, vec), index) => (index, id)}.collectAsMap()


    val irm = new IndexedRowMatrix(rddIndex.map{case ((id, vec), index) => IndexedRow(index, vec)})
      .toCoordinateMatrix().transpose().toRowMatrix().columnSimilarities()


    irm.entries.map(e => (indexMap(e.i), indexMap(e.j), e.value))



//    val lm = irm.toIndexedRowMatrix.toBlockMatrix.toLocalMatrix
//
//    import spark.implicits._
//    val cols = (0 until lm.numCols).toSeq
//
//    val df = lm.transpose
//      .colIter.toSeq
//      .map(_.toArray)
//      .toDF("arr")
//
//    val df2 = cols.foldLeft(df)((df, i) => df.withColumn("_" + (i+1), $"arr"(i)))
//      .drop("arr")


//    val irm = new IndexedRowMatrix(tableA.rdd.map {
//      Row(_, v: Array[Float]) =>
//      org.apache.spark.mllib.linalg.Vectors.fromML(v)
//    }.zipWithIndex.map { case (v, i) => IndexedRow(i, v) })


//
//
//    new IndexedRowMatrix(tableA.rdd.map {
//      Row(_, v:org.apache.spark.ml.linalg.DenseVector) =>
//      org.apache.spark.mllib.linalg.Vectors.fromML(v)
//    }.zipWithIndex.map {case (v, i) => IndexedRow(i, v)})

//    import spark.implicits._
//    val cols = (0 until lm.numCols).toSeq
//
//    val df = lm.transpose
//      .colIter.toSeq
//      .map(_.toArray)
//      .toDF("arr")
//
//    val df2 = cols.foldLeft(df)((df, i) => df.withColumn("_" + (i+1), $"arr"(i)))
//      .drop("arr")



    spark.stop()
  }

}
