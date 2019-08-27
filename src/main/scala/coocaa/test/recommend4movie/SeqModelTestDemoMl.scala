package coocaa.test.recommend4movie

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.functions.{col, lit, split}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by Joe.Kwan on 2018/12/27 11:09.
  */
object SeqModelTestDemoMl {


  def main(args: Array[String]): Unit = {



    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    /**
      * 导入配置文件
      */
    val sparkConf = new SparkConf()

    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.rdd.compress", "true")
    sparkConf.set("hive.metastore.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
    sparkConf.set("spark.sql.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
    val spark = SparkSession.builder()
      .config(sparkConf)
      .appName(s"${this.getClass.getSimpleName}")
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()


    /**
      * 导入数据
      */

//    val oriDf = spark.read.format("csv")
//      .option("header", "true")
//      .option("inferSchema", "true")
//      .option("mode", "DROPMALFORMED")
//      .load("F:\\projectP\\coocaa\\seqModel\\src\\test_data.csv")
//      .withColumnRenamed("eq", "seq")
//      .select("seq").withColumn("seq", regexp_replace(col("seq"), " |,", ""))
//      .withColumn("seq", split(col("seq"), "[|]")).select("seq")


    /**
      * 从hive中计算
      *
      */
    val n_day_before= "2018-12-24"
    val date_Str = "2018-12-26"
    val oriDF = spark.sql(
      s"""
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
         |      partition_day >= '${n_day_before}' and partition_day <= '${date_Str}' and
         |      video_id is not null and
         |      category = '电影'
         |      group by mac,`name` ) name_without_space --tmp_table01
         |    group by mac, `name` ) name_dropduplicates --tmp_table02
         |  group by mac ) b --tmp_table03
         |where length(b.seq) >=10
      """.stripMargin)
      .select("seq")
      //      .withColumn("seq", regexp_replace(col("seq"), " |,", ""))   // sql中已经去掉了特殊符号(测试版需要对中文名称处理)
      .withColumn("seq", split(col("seq"), "[|]")).select("seq")



    /**
      * 适用于doc2vec???
      */

    val word2Vec = new Word2Vec()
        .setInputCol("seq")
        .setMaxIter(6)
        .setMinCount(5)
        .setNumPartitions(8)
        .setOutputCol("result")
        .setSeed(1)
        .setStepSize(0.001)
        .setVectorSize(200)
        .setWindowSize(7)

    /**
      * 训练模型
      */
    val model = word2Vec.fit(oriDF)

//    val result = model.transform(oriDF)

    /**
      * 生成词向量
      */
    val vecs = model.getVectors

    import spark.implicits._
    val playsArray: Array[String] = vecs.select("word").map(r => r.getString(0)).collect.toArray


    /**
      * 生成结果
      */
    val schema = new StructType(Array(
      new StructField("word", StringType, true),
      new StructField("similarity", DoubleType, false),
      new StructField("ori_word", StringType, false)
    ))

    var allPlaysDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    for (eachEle <- playsArray) {
      val eachEleDF = model.findSynonyms(eachEle, 20).withColumn("ori_word", lit(eachEle))
      allPlaysDF = allPlaysDF.union(eachEleDF)
    }

    println("allPlaysDF is already finished!")


    val outPutDF = allPlaysDF.select("ori_word", "word", "similarity")
      .withColumnRenamed("ori_word", "target_movie")
      .withColumnRenamed("word", "compared_movie")

    /**
      * result to my sql
      */
    val url = "jdbc:mysql://192.168.1.57:3307/test?useUnicode=true&characterEncoding=UTF-8"
    val props = new Properties()
    props.put("user","hadoop2")
    props.put("password","pw.mDFL1ap")
    props.put("driver", "com.mysql.jdbc.Driver")
    outPutDF.write.mode("Overwrite").jdbc(url,"sim_test_gy",props)



    spark.stop()


  }

}
