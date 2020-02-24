import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.functions._


import breeze.linalg.{SparseVector => BV}
import org.apache.spark.ml.linalg.{Vectors, SparseVector => SparkVector}


/**
 * Created by Joe.Kwan on 2020/1/6
 */
object MultiHotEncoder {

  def toBreeze(v: SparkVector) = BV(v.toArray)
  def fromBreeze(bv: BV[Double]) = Vectors.dense(bv.toArray)
  def sparse_add(v1: SparkVector, v2: SparkVector): SparkVector = fromBreeze(toBreeze(v1) + toBreeze(v2)).toSparse


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


    /**
     * 将Seq序列转换成spark dataframe
     *
     */
    import spark.implicits._
    val df = Seq(
      (0, 0, 5),
      (0, 1, 3),
      (0, 3, 3),
      (1, 3, 2),
      (1, 4, 3),
      (1, 5, 1),
      (2, 1, 3),
      (2, 2, 5),
      (2, 3, 3),
      (3, 4, 4),
      (3, 5, 5)
    ).toDF("user", "movie", "rating")

    df.show()

    //
    val encoder = new OneHotEncoder()
      .setInputCol("movie")
      .setOutputCol("encoded")
      .setDropLast(false)
        .transform(df)

    encoder.show(false)

    encoder.rdd.map {row =>

      val user = row.getAs[Int]("user")
      val movie_vector = row.getAs[org.apache.spark.ml.linalg.SparseVector]("encoded")
      (user, movie_vector)

    }.reduceByKey((a, b) => sparse_add(a, b)).map {row =>
      val user = row._1
      val movie_vector = row._2
      (user, movie_vector.toDense)
    }.toDF("user", "multi_hot").show(false)










    // indexing columns
//    val stringColumns = Array("movie")
//    val index_transformers: Array[org.apache.spark.ml.PipelineStage] = stringColumns.map()
//
//    val pipeline = new Pipeline()
//      .setStages(index_transformers ++ one_hot_encoders)
//
//    val model = pipeline.fit(df)
//    model.transform(df)

//    val movieIndexer = new StringIndexer().setInputCol("movie").setOutputCol("movieIndex").fit(df)
//    val movieIndexed = movieIndexer.transform(df)
//
//
//    val encoder = new OneHotEncoder()
//      .setInputCol("movieIndex")
//      .setOutputCol("testOutput")
//      .setDropLast(false)
//
//    val encoded = encoder.transform(movieIndexed)
//
//    encoded.show()



    spark.stop()


  }

}
