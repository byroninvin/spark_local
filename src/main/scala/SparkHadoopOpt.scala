import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}

import org.apache.hadoop.fs.{FileStatus, FsStatus, Path}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
 * Created by Joe.Kwan on 2020/2/6
 */
object SparkHadoopOpt {

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

    // /data/model/GbdtLRModel/NRTAlbum
//    val path = new Path("hdfs://coocaadata/user/guanyue/spark_model_checkpoint/")
//    val path = new Path("/data/model/GbdtLRModel/NRTAlbum_demo2")
//    val hadoopConf = spark.sparkContext.hadoopConfiguration
//
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    val testBool = hdfs.exists(path)
//    println(testBool)
//
//    val modelListStatus: Array[FileStatus] = hdfs.listStatus(path)
//    for (each <- modelListStatus) {
//      val model_file_create_time = each.getModificationTime
//
//      val sdf = new SimpleDateFormat("yyyy-MM-dd")
//      val model_file_create_date_str = sdf.format(model_file_create_time)
//
//
//
//      val each_path = each.getPath
//
//
//      if (model_file_create_date_str<"2020-05-23") {
//        println(model_file_create_date_str)
//        println(each_path)
//        hdfs.delete(each_path, true)
//        println("ok")
//      }
//    }

    /**
     * 从目标地址中删除一个判定时间之前的全部模型文件
     * @param spark SparkSession
     * @param model_path 模型存放的目录-此类模型存放的父目录
     * @param target_date_str 比较时间
     */
    def rmHistoryModels(spark: SparkSession, model_path: String, target_date_str: String) = {
      // 只针对特定目录
      if (model_path.startsWith("/data/model")) {
        // 创建 fs
        val path = new Path(model_path)
        val hadoopConf = spark.sparkContext.hadoopConfiguration
        val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
        // 判断一下当前路径是否存在
        if (hdfs.exists(path)) {
          println(s"要删除的文件目录存在！模型目录为：$path")
          val modelListStatus: Array[FileStatus] = hdfs.listStatus(path)
          for (i <- modelListStatus) {
            // 判断每个成员的生成日期
            val model_file_create_time = i.getModificationTime()
            val sdf = new SimpleDateFormat("yyyy-MM-dd")
            val model_file_create_date_str = sdf.format(model_file_create_time)
            // 如果模型文件创建的日期小于目标时间的话，就删除该子文件夹
            val each_path = i.getPath
            println(s"其中模型文件创建时间为：$model_file_create_date_str")
            println(s"其中模型文件的路径为：$each_path")

            if (model_file_create_date_str< target_date_str) {
              println(s"判断的删除模型的日期为：$target_date_str")
              println(s"模型创建时间为：$model_file_create_date_str, 小于删除模型的规范日期：$target_date_str, 故将此模型文件全部删除，路径为：$each_path")
//              hdfs.delete(each_path,true)
              println(s"子文件目录: $each_path, 已经成功删除")
            }
          }
        } else { println(s"请查看你设置的目录：$path ,是否存在！")}
      } else { println(s"请查看你设置的目录：$model_path ,是否有权利去操作！")}
    }



//    rmHistoryModels(spark, "/data/model/GbdtLRModel/NRTAlbum_demo2", "2020-05-23")

    /**
     * 获取一个目录下的最大日期
     */
    val path_str = "/apps/hive/warehouse/user_predict.db/nrt_platform_movie_tag_feature_v1/"


    /**
     * 根据parquet的day=字段文件来获取最大分区
     * @param spark
     * @param path_str
     * @return
     */
    def getParquetFilePathMaxDate(spark: SparkSession, path_str: String) = {
      val path = new Path(path_str)
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

      val modelListStatus: Array[FileStatus] = hdfs.listStatus(path)
      val dateList = new ListBuffer[String]()
      for (each <- modelListStatus) {
        val model_file_create_time: Path = each.getPath
        val lastData = model_file_create_time.toString.split("=").last
        dateList += lastData
      }
      dateList
    }


    val test = getParquetFilePathMaxDate(spark, path_str)
    println(test)


    /**
     * 判断3者最大相同时间
     */
    val list_a = new ListBuffer[String]()
    val list_b = new ListBuffer[String]()
    val list_c = new ListBuffer[String]()

    list_a += ("2020-06-04", "2020-06-03", "2020-06-09", "2020-06-01")
    list_b += ("2020-06-05", "2020-06-02", "2020-06-10", "2020-06-09")
    list_c += ("2020-06-03", "2020-06-01", "2020-06-09", "2020-06-02")


    /**
     *
     * @param list_of_list
     * @return
     */
    def chooseThreeListBufferMaxIntersectDate(list_of_list: ListBuffer[String] *) = {
      val list_of_list_size = list_of_list.size
      val chosen_list = new ListBuffer[String]()
      val allElementList= list_of_list.flatten.map((_,1)).groupBy(_._1).mapValues(_.size).toList.sortBy(_._2).reverse
      for (each <- allElementList) {
        if (each._2==list_of_list_size) {
          chosen_list += each._1
        }
      }
      chosen_list.max
    }

    val test_test_list = new ListBuffer[ListBuffer[String]]
    test_test_list += (list_a, list_b, list_c)
    println(test_test_list)

    val result = chooseThreeListBufferMaxIntersectDate(test_test_list:_*)
    println(result)


    spark.stop()

  }

}
