package ml.recommend2demo

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Created by Joe.Kwan on 2018/9/9. 
  */
object ItemBaseCftest2 {

  case class Cfdata(userId: Long, itemId: Long, rating: Double)

  def parsetoMatrix(data: DataFrame): CoordinateMatrix = {
    val parseData = data.rdd.map {
      case Row(user: Long, item: Long, rate: Double) =>
        MatrixEntry(user, item, rate.toDouble)
    }
    new CoordinateMatrix(parseData)
  }

  def StandardCosine(matrix: CoordinateMatrix): RDD[MatrixEntry] = {
    val similarity = matrix.toIndexedRowMatrix().columnSimilarities()
    val sim = similarity.entries
    sim
  }


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]").appName("ItemBaseCftest2")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //      .config("spark.rdd.compress", "true")
      //      .config("spark.speculation.interval", "10000ms")
      //      .config("spark.sql.tungsten.enabled", "true")
      .getOrCreate()

    val lines: RDD[String] = spark.sparkContext.textFile("F:\\projectP\\Python3WithNoteBook\\Recommender_system\\recommendation_system_codes\\movielens\\ml-1m\\ratings.dat")

    val cfData: RDD[Cfdata] = lines.map(line => {
      val fields = line.split("::")
      val userId = fields(0).toLong
      val itemId = fields(1).toLong
      val rating = fields(2).toDouble
      Cfdata(userId, itemId, rating)
    })

    import spark.implicits._
    val cfDf: DataFrame = cfData.toDF
    val cfDs: Dataset[Cfdata] = cfData.toDS


    val Array(training, test): Array[Dataset[Cfdata]] = cfDs.randomSplit(Array(0.8, 0.2))



    val test1: CoordinateMatrix = parsetoMatrix(training.toDF)
    val itemSim: DataFrame = StandardCosine(test1).collect.toIndexedSeq.toDF("itemX", "itemY", "sim")

    training.createOrReplaceTempView("training")
    itemSim.createOrReplaceTempView("itemSim")
    test.createOrReplaceTempView("test")


    val testItemSim = spark.sql(
      """
        |select test.userId, test.itemId, test.rating testRating, training.rating, sim.sim
        |from test
        |left join training on test.userId = training.userId
        |left join itemSim sim on test.itemId=sim.itemX and training.itemId=sim.itemY
      """.stripMargin).toDF()



    training.unpersist()
    itemSim.unpersist()
    test.unpersist()

    testItemSim.createOrReplaceTempView("testAndSim")

    //预测评分
    val sqlRank = "select userId,itemId,testRating,rating,sim," +
      "rank() over (partition by userId,itemId order by sim desc) rank\n" +
      "from testAndSim"
    val testAndPre = spark.sql(
      "select userId,itemId,first(testRating) rate,nvl(sum(rating*sim)/sum(abs(sim)),0) pre\n" +
        "from( " +
        "  select *" +
        "  from  (" + sqlRank + ") t " +
        s" where rank <= 50 " +
        ") w " +
        "group by userId,itemId"
    ).toDF()


    // 将数据Jdbc存入到数据库
//    val props = new Properties()
//    props.put("user", "root")
//    props.put("password", "741852")
//    //
//    itemSim.write.mode("Overwrite")
//      .jdbc("jdbc:mysql://localhost:3306/test", "itemSim",props)

//    itemSim.write.json("F:\\projectP\\LearningPython3\\Recommender_system\\recommendation_system_codes\\movielens\\ml-1m\\test123")

    testItemSim.show()
    spark.stop()

  }

}
