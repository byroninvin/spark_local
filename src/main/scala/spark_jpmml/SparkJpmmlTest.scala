package spark_jpmml

import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.dmg.pmml.PMML
import org.jpmml.sparkml.PMMLBuilder
import util.{Breeze2SparkConverter, Spark2BreezeConverter}

/**
 * Created by Joe.Kwan on 2020/6/13
 */
object SparkJpmmlTest {


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


//    val schema =
//    val lrModel: LogisticRegressionModel = LogisticRegressionModel.load(lr_model_path)
//
//
//    PMML pmml = new PMMLBuilder(schema, lrModel)


    spark.stop()
  }

}
