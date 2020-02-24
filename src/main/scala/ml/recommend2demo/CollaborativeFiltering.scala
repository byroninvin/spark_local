package ml.recommend2demo

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.broadcast.Broadcast


object CollaborativeFiltering {


  def getResource(sc: SparkContext, table: String, day: String) = {
    val hiveContext = new HiveContext(sc)
    import hiveContext.sql
    val resource = sql("select "
    + "uid,"
    + "aid,"
    + "cnt"
    + " from " + table + " where dt ='" + day + "'")
    resource
  }


  def getCosineSimilarity(inputRdd: RDD[(String, String, Double)]): RDD[(String, (String, Double))] ={

    // RDD[(uid, (aid, score))]
    val user_item_score = inputRdd.map(f => (f._1, (f._2, f._3)))
    //
    val item_score_pair = user_item_score.join(user_item_score)
      .map(f => ((f._2._1._1, f._2._2._1), (f._2._1._2, f._2._2._2)))

    val item_pair_all = item_score_pair.map(f => (f._1, f._2._1 * f._2._2)).reduceByKey(_ + _)

    val item_pair_XX_YY = item_pair_all.filter(f => f._1._1 == f._1._2)

    val item_pair_XY = item_pair_all.filter(f => f._1._1 != f._1._2)

    val item_XX_YY = item_pair_XX_YY.map(f =>(f._1._1, f._2))

    val item_XY_XX = item_pair_XY.map(f => (f._1._1, (f._1._1, f._1._2, f._2))).join(item_XX_YY)

    val item_XY_XX_YY = item_XY_XX.map(f => (f._2._1._2, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._2))).join(item_XX_YY)

    val item_pair_XY_XX_YY = item_XY_XX_YY.map(f => (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._2))

    val item_pair_sim = item_pair_XY_XX_YY.map(f => (f._1, (f._2, f._3 / math.sqrt(f._4 * f._5))))

    item_pair_sim
  }

  def recommend(inputRdd: RDD[(String, String, Double)],
                item_sim_bd: Broadcast[scala.collection.Map[String, List[(String, Double)]]],
                topN: Int=50) = {

    val user_item_score = inputRdd.map(f => ((f._1, f._2), f._3))

    val user_item_simScore = user_item_score.flatMap(
      f => {
        val items_sim = item_sim_bd.value
          .getOrElse(f._1._2, List(("0", 0.0)))
        for (w <- items_sim) yield ((f._1._1, w._1), w._2 * f._2)
      }
    ).filter(_._2 > 0.03)

    val user_item_rank = user_item_simScore.reduceByKey(_ + _, 1000)

    val user_items_ranks = user_item_rank.subtractByKey(user_item_score)
      .map(f => (f._1._1, (f._1._2, f._2))).groupByKey()

    val user_items_ranks_desc = user_items_ranks.map(f => {
      val item_rank_list = f._2.toList
      val item_rank_desc = item_rank_list.sortWith((x, y) => x._2 > y._2)
      (f._1, item_rank_desc.take(topN))
    })
    user_items_ranks_desc

  }

  def encodeToJson(recTopN: (String, List[(String, Double)])) = {
    val mytype = "u2a"
    val mytpye_ = "\"" + "mytype" + "\"" + ":" + mytype + "\""
    val uid = recTopN._1
    val uid_ = "\"" + "uid" + "\"" + ":" + uid + "\""
    val aid_score = recTopN._2
    val aids_ = new StringBuilder().append("\"" + "aids" + "\"" + ":[")
    for (v <- aid_score) {
      val aid = v._1
      val score = v._2
      val aid_score = "[" + "\"" + aid + "\"" + "," + score + "]"
      aids_.append(aid_score + ",")
    }
    aids_.deleteCharAt(aids_.length - 1).append("]")
    val result = "{" + mytpye_ + "," + uid_ + "," + aids_.toString() + "}"
    result
  }



  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("CollaborativeFiltering")
      .config("spark.executor.memory","2g")
      .config("spark.locality.wait","60")
      .getOrCreate()

//    val sc = spark.sparkContext

    val lines: RDD[String] = spark.sparkContext.textFile("F:\\projectP\\Python3WithNoteBook\\Recommender_system\\recommendation_system_codes\\movielens\\ml-1m\\ratings.dat")
//    val cfData: RDD[CfStruct] = lines.map(line => {
//      val fields = line.split("::")
//      val userId = fields(0)
//      val itemId = fields(1)
//      val rating = fields(2).toDouble
//      CfStruct(userId, userId, rating)
//    })
    val inputRdd = lines.map(line => {
      val fields = line.split("::")
      (fields(0), fields(1), fields(2).toDouble)
    })



    val item_sim: RDD[(String, (String, Double))] = getCosineSimilarity(inputRdd)
    item_sim.cache()

    val item_sim_rdd = item_sim.filter(f => f._2._2 > 0.05).groupByKey().map(
      f => {
        val item = f._1
        val items_score = f._2.toList
        val items_score_desc = items_score.sortWith((x, y) => x._2 > y._2)
        (item, items_score_desc.take(40))
      }).collectAsMap()

    val item_sim_bd: Broadcast[scala.collection.Map[String, List[(String, Double)]]] = spark.sparkContext.broadcast(item_sim_rdd)

    val recTopN = recommend(inputRdd, item_sim_bd, 50)

//    recTopN.map(encodeToJson(_)).take(10).foreach(println)
//    recTopN.map(encodeToJson(_)).take(100).foreach(println)


    item_sim.take(10).foreach(println)
    item_sim_rdd.take(10).foreach(println)
    spark.stop()
  }

}
