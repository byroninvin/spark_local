//package ml.fm
//
//import org.apache.spark.SparkConf
//import org.apache.spark.mllib.util.MLUtils
//import org.apache.spark.sql.SparkSession
//
///**
// * Created by Joe.Kwan on 2020/1/29
// */
//object TestFM extends App {
//
//  override def main(args: Array[String]): Unit = {
//
//    val sparkConf = new SparkConf()
//    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    sparkConf.set("spark.rdd.compress", "true")
//    sparkConf.set("hive.metastore.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
//    sparkConf.set("spark.sql.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
//
//    val spark = SparkSession.builder()
//      .config(sparkConf)
//      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
//      .enableHiveSupport()
//      .master("local[*]")
//      .getOrCreate()
//
//    val training = MLUtils.loadLibSVMFile(spark.sparkContext, "").cache()
//
//    val fm1 = FMWithSGD.train(
//      training,
//      task = 1,
//      numIterations = 100,
//      stepSize = 0.15,
//      miniBatchFraction = 1.0,
//      dim=(true , true, 4),
//      regParam = (0, 0, 0),
//      initStd = 0.1
//    )
//
//    val fm2 = FMWithLBFGS.train(
//      training,
//      task = 1,
//      numIterations = 20,
//      numCorrections = 5,
//      dim=(true, true, 4),
//      regParam = (0, 0, 0),
//      initStd = 0.1
//    )
//
//
//    spark.stop()
//
//  }
//
//}
