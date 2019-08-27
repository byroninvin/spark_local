package coocaa.test.fpgrowth

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by Joe.Kwan on 2018/12/10 10:14. 
  */
object FpGrowthMlCoocaaTest {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)


    val sparkConf = new SparkConf()
    //args.map(_.split("=")).filter(_.size==2).map(x=>sparkConf.set(x(0),x(1)))
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


    /**
      * 从集群上导入数据
      */
    import spark.implicits._
    val oriDf = spark.sql(
      """
        |select mac as mac, seq as seq
        |from (
        |  select mac as mac,concat_ws('|', collect_set(name)) as seq
        |  from(
        |    select mac, `name`
        |    from (
        |      select
        |      upper(mac) as mac,
        |      regexp_replace(`name`, " |,", "") as `name`  --去掉特殊符号
        |      from edw.edw_player
        |      where dur > 300 and
        |      partition_day >= '2018-12-09' and partition_day <= '2018-12-09' and
        |      video_id is not null and
        |      category = '电影'
        |      group by mac,`name` ) name_without_space --tmp_table01
        |    group by mac, `name` ) name_dropduplicates --tmp_table02
        |  group by mac ) b --tmp_table03
        |where length(b.seq) >=10
      """.stripMargin)

    oriDf.cache()

    /**
      * 将数据整理成FpGrowth形式的格式
      */
    val oriDfSplit = oriDf
      .withColumn("newColumn", split(oriDf("seq"), "[|]")).select("newColumn")

    val trainingSet: DataFrame = oriDfSplit.withColumn("newColumnArray", sort_array(oriDfSplit("newColumn"))).select("newColumnArray")


    trainingSet.show()

    val fpgrowth = new FPGrowth()
      .setItemsCol("newColumnArray")    //
      .setMinSupport(0.0001)      // 设置最小支持度
      .setMinConfidence(0.004)   // 设置最小的置信度
    //
    //
    val model = fpgrowth.fit(trainingSet)




    model.freqItemsets.show()
    model.associationRules.show()



    spark.stop()

  }
}
