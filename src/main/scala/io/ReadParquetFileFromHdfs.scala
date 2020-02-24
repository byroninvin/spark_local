package io

import org.apache.spark.sql.SparkSession

/**
 * Created by Joe.Kwan on 2019-11-18 17:21. 
 */
object ReadParquetFileFromHdfs {

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
     * /user/guanyue/recommendation/dataset/bpr/movie_rating_dataset.parquet
     */

    val parquet_file_path = "/user/guanyue/recommendation/dataset/bpr/movie_rating_dataset.parquet"
    val parquetFileDF = spark.read.parquet(parquet_file_path)

    val embd_result_path = "/user/guanyue/recommendation/model_result/lfm/lfm_user_cosine_sim_20191118.parquet"

    val embd_result_DF = spark.read.parquet(embd_result_path)

    embd_result_DF.printSchema()




    spark.stop()
  }

}
