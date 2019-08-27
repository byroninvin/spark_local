package coocaa.test.recommend4movie

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, current_date, monotonically_increasing_id, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  * Created by Joe.Kwan on 2018/12/28 15:48.
  */
object W2vTestDemo {

  case class ItemToItem(word: String, rec_word: String, sim: Float)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]").appName("w2v")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //      .config("spark.rdd.compress", "true")
      //      .config("spark.speculation.interval", "10000ms")
      //      .config("spark.sql.tungsten.enabled", "true")
      .getOrCreate()


    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)

//    val result = model.transform(documentDF)
//    result.collect().foreach { case Row(text: Seq[_], features: Vector) =>
//      println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n") }

    import spark.implicits._
    val vecs = model.getVectors


    /**
      * 借鉴ALS的方式来计算相似度
      */

    val itemIndex = vecs.select("word").rdd.zipWithIndex().map(x => (x._1.getString(0), x._2.toInt)).cache()

    // ml.DenseVector => array
    // 这块是重点
    val toArr: Any => Array[Float] = _.asInstanceOf[DenseVector].toArray.map(_.toFloat)
    val toArrUdf = udf(toArr)
    val dataWithFeaturesArr = vecs.withColumn("features_arr",toArrUdf(col("vector"))).withColumn("word_id", monotonically_increasing_id)
//      .select("features_arr")
    dataWithFeaturesArr.show()

    val itemFeature: Array[(Int, mutable.WrappedArray[Float])] = dataWithFeaturesArr.select("word_id", "features_arr").rdd.map(
      row => {
        val item = row.getLong(0).toInt
        val vec = row.get(1).asInstanceOf[mutable.WrappedArray[Float]]
        (item, vec)
      }
    ).collect()


    val rank: Int = 3
    val numItems: Int = dataWithFeaturesArr.select("word_id").count().toInt
    val itemVectors: Array[Float] = itemFeature.flatMap(x => x._2)
    val itemIndex_tmp: collection.Map[String, Int] = itemIndex.collectAsMap()
    val blasSim = new BlasSim(numItems, rank, itemVectors, itemIndex_tmp)


    val itemString: RDD[(Int, String)] = itemIndex.map(x => (x._2, x._1)).repartition(100)

    val item_sim_rdd: RDD[(String, List[(String, Double)])] = itemString.map(x => {
      (x._2, blasSim.getCosinSimilarity((itemFeature(x._1)._2.toVector), 50, None).toList)
    })

    // (i, List())to df
    val outPutDF: DataFrame = item_sim_rdd.flatMap(line => {
      val word = line._1
      val sim_list = line._2
      for (w <- sim_list) yield (word, w._1, w._2)

    }).toDF("ori_item", "rec_item", "sim")
      .filter("ori_item <> rec_item")
      .withColumn("partition_day", current_date())
      .selectExpr("cast(ori_item as String) ori_item",
      "cast(rec_item as String) rec_item",
      "cast(sim as Double) sim",
      "cast(partition_day as String) partition_day")


    println(outPutDF.printSchema)




    /**
      * 矩阵方式一次性生成结果集
      */
//    val user_index = vecs.select("word").rdd.zipWithIndex().map(x => (x._1, x._2.toLong))
//
//    val index_df = vecs.select("word").withColumn("word_id", monotonically_increasing_id)
//    // user_index.collect
//    // idnex_df
//    index_df.createOrReplaceTempView("index_df")
//
//    val rddtest = vecs.select("vector").rdd.map{x:Row => x.getAs[Vector](0)}
////    rddtest.collect()
//    val indexedRDD = rddtest.zipWithIndex.map{ case (value, index) => IndexedRow(index,  org.apache.spark.mllib.linalg.Vectors.fromML(value))}
//    // indexedRDD.collect()
//
//    val matrix = new IndexedRowMatrix(indexedRDD)
//
//    val dist = matrix.toCoordinateMatrix.transpose().toIndexedRowMatrix().columnSimilarities()
//    //
//    val sim = dist.entries
//
//    // sim.collect.toIndexedSeq.toDF("itemX", "itemY", "sim").filter("sim > 0").show(500)
//
//    sim.collect.toIndexedSeq.toDF("itemX", "itemY", "sim").filter("sim > 0").createOrReplaceTempView("sim_table")
//
//
//    spark.sql(
//      """
//        |select b.word as ori_item, c.word as rec_item, a.sim, CAST(current_date() AS STRING) as partition_day
//        |from sim_table a
//        |left join index_df b
//        |on a.itemX = b.word_id
//        |left join index_df c
//        |on a.itemY = c.word_id
//        |where b.word = 'heard'
//        |order by sim DESC
//      """.stripMargin).printSchema()

//    spark.sql(
//      """
//        |select b.word as ori_item, c.word as rec_item, a.sim, current_date() as partition_day
//        |from sim_table a
//        |left join index_df b
//        |on a.itemX = b.word_id
//        |left join index_df c
//        |on a.itemY = c.word_id
//        |where b.word = 'heard'
//        |order by sim DESC
//      """.stripMargin).show()







    /**
      * 一边计算一遍插入数据
      */

//    println("马上开始生成结果了")
//    val url = "jdbc:mysql://192.168.1.57:3307/test?useUnicode=true&characterEncoding=UTF-8"
//    val props = new Properties()
//    props.put("user","hadoop2")
//    props.put("password","pw.mDFL1ap")
//    props.put("driver", "com.mysql.jdbc.Driver")
//
//    for (eachEle <- playsArray) {
//      val eachEleDF = model.findSynonyms(eachEle, 20).withColumn("ori_word", lit(eachEle))
//      eachEleDF.write.mode("append").jdbc(url, "sim_test_gy2", props)
//    }














    spark.stop()

  }

}
