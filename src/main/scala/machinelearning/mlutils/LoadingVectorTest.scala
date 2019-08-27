package machinelearning.mlutils

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.SparseVector

/**
  * Created by Joe.Kwan on 2019-8-21 18:05. 
  */
object LoadingVectorTest {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()


    import spark.implicits._
    val talbe2=spark.sql(
      s"""
         |select did,features from
         |user_predict.user_features_did
         |limit 20
   """.stripMargin)

    talbe2.rdd.map(f=>(f.getAs[String]("did"),f.getAs[SparseVector]("features"))).toDF("did","feature").show()


    spark.stop()

  }

}
