import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.{SparseVector, Vectors}
import org.apache.spark.sql.SparkSession
import util.Spark2BreezeConverter

/**
 * Created by Joe.Kwan on 2020/6/1
 */
object SparkCreateEmptySparse {


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

    val day="2020-05-30"

    val vid_features = spark.sql(s"""select coocaa_v_id, movie_tag_feature from test.wyh_movie_tags_feature where day='$day' limit 1""")
      .selectExpr("coocaa_v_id as vid", "movie_tag_feature as vid_feature")

    vid_features.show()

    // 如果没有匹配上的话就给一个全零向量
    val allZeroSize: Int = vid_features.rdd.map{ row =>
      val vid_feature_size  = row.getAs[org.apache.spark.ml.linalg.SparseVector]("vid_feature").size
      vid_feature_size
    }.take(1).last

    println(allZeroSize)

    val allZeroSparse: SparseVector = Vectors.zeros(allZeroSize).toSparse

    println(allZeroSparse)



    spark.stop()
  }

}
