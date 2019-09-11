package demo.sparksql.functions

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by Joe.Kwan on 2019-8-27 17:37. 
  */
object SparkApiExplodeFunc {

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

    import spark.implicits._

    val testDF = Seq(
      ("00153", "A", "t,y", 0.36),
      ("00154", "B", "y,t,z", 0.36),
      ("00155", "C", "y,r,b", 0.36),
      ("00156", "D", "y", 0.36),
      ("00157", "A", "t,a", 0.36)
    ).toDF("did", "vid", "source", "rating1")

    testDF.show()


    /**
      *
      */
    testDF.withColumn("splitted",split(col("source"),","))
      .withColumn("exploded",explode(col("splitted")))
      .filter(col("exploded").isin(Array("t","z"):_*))
      .select("did","vid","exploded", "rating1").show(false)



    spark.stop()



  }

}
