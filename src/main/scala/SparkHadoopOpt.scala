import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}

import org.apache.hadoop.fs.{FileStatus, FsStatus, Path}
import org.apache.spark.sql.SparkSession

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
    val path = new Path("/user/guanyue/spark_model_checkpoint/")
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val testBool = hdfs.exists(path)
    println(testBool)

    /**
     * 遍历里面的文件
     *
     */
    val testStatus: Array[FileStatus] = hdfs.listStatus(path)
    for (i <- testStatus) {
      val each = i.getPath()
      val time = i.getModificationTime()
      println(new Timestamp(time).toString())
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val date_str: String = sdf.format(time)
      println("2020-05-14" > date_str)
      println(each)
    }


    def rmHistoryModels(model_path: String, target_date_str: String) = {
      // 只针对特定目录
      if (model_path.startsWith("/data/model")) {
        // 创建 fs
        val path = new Path(model_path)
        val hadoopConf = spark.sparkContext.hadoopConfiguration
        val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
        // 判断一下当前路径是否存在
        if (hdfs.exists(path)) {
          val modelListStatus: Array[FileStatus] = hdfs.listStatus(path)
          for (i <- modelListStatus) {
            // 判断每个成员的生成日期
            val model_file_create_time = i.getModificationTime()
            val sdf = new SimpleDateFormat("yyyy-MM-dd")
            val model_file_create_date_str = sdf.format(model_file_create_time)
            // 如果模型文件创建的日期小于目标时间的话，就删除该子文件夹
            val each_path = i.getPath
            if (model_file_create_date_str< target_date_str) {
              hdfs.delete(each_path,true)
              println(s"子文件目录: $each_path, 已经成功删除")
            }
          }
        } else { println(s"请查看你设置的目录：$path ,是否存在！")}
      } else { println(s"请查看你设置的目录：$path ,是否有权利去操作！")}
    }


    spark.stop()

  }

}
