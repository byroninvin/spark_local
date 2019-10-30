package coocaa.test

import java.io.FileNotFoundException

import breeze.numerics.abs
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{AnyDataType, DataType, StructType}

object spark_local_test_16 {
  case class infoss(mac:String,village:String,data:scala.collection.mutable.Map[String,Any])
  case class MacInfo(mac:String, key:String,data_date:String,exposure:String,click:String)
  case class Info(mac:String,key:Array[String])
  case class ResultInfo(mac:String,key:String)
  //case class MacApply(app_id:Long,order_no:String,tv_info:String,movement:String,app_num:Long,barcode_start:String,barcode_end:String,mac_add_start:String,mac_add_end:String,start_time:String,createtime:String,createman :String,department:String,owner_name:String,owner_id:String,state:Long,to_config:Long,mac_start_decimal:String,mac_end_decimal:String,post_oss:String,delete_flag:Int,brand_id:Long,oem_type_id:Long)
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    //args.map(_.split("=")).filter(_.size==2).map(x=>sparkConf.set(x(0),x(1)))
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.rdd.compress", "true")
    sparkConf.set("hive.metastore.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
    sparkConf.set("spark.sql.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
    //sparkConf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    val spark = SparkSession.builder()
      .config(sparkConf)
      .appName(this.getClass.getSimpleName).enableHiveSupport()
      .master("local[2]")
      .getOrCreate()



    val testSeq = Seq("mac,village","mac,555")

    var testMap = scala.collection.mutable.Map[String,Any]()
    testMap.put("cc",1905)
    println(testMap)
    import spark.implicits._
    spark.sparkContext.parallelize(testSeq).map(line=>{
     val ele = line.split(",")
      val mac=ele(0)
      val village=ele(1)
      val testMap = scala.collection.mutable.Map.empty[String,Any]
      testMap.put("cc" , 1905)
      infoss(mac,village,testMap)
    }).collect().foreach(println)

//      .toDF().createOrReplaceTempView("temp")
//    spark.sql("select * from temp").show(false)


    spark.stop()
  }

  def toHashcode11(str:String):Int={
    val output = abs(str.hashCode)%99
    output
  }


  def toHashcode12(str:String):Int={
    val output = abs(str.hashCode)%100
    output
  }

  /**
    * 功能：判断集群文件系统的文件大小以及传入目录下的文件大小
    * @param fs
    * @param path 文件目录或者具体文件路径
    * @return
    */
  def getPathOrFileLen(fs: FileSystem, path: String) : Long ={
    var len:Long=0
    try {
      if(fs.isDirectory(new org.apache.hadoop.fs.Path(path))){
        for(f <-fs.listStatus(new org.apache.hadoop.fs.Path(path))){
          len+=f.getLen
        }
      }else{
        len=fs.getFileStatus(new org.apache.hadoop.fs.Path(path)).getLen
      }
    }catch {
      case ex: FileNotFoundException => println(ex)
    }
    return len
  }


}
