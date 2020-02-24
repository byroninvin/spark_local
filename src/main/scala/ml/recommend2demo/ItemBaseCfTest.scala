package ml.recommend2demo

import java.util.Properties

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Created by Joe.Kwan on 2018/9/8.
  */
object ItemBaseCfTest {


  case class Cfdata(userId: Long, itemId: Long, rating: Double)

  //计算相似度矩阵
  //评分数据转换成矩阵
  def parsetoMatrix(data: DataFrame): CoordinateMatrix = {
    val parseData = data.rdd.map {
      case Row(user: Long, item: Long, rate: Double) =>
        MatrixEntry(user, item, rate.toDouble)
    }
    new CoordinateMatrix(parseData)
  }

  //计算相似度矩阵 MartixEntry(itemId, itemId, simRating)
  def StandardCosine(matrix: CoordinateMatrix):RDD[MatrixEntry] = {
    val similarity = matrix.toIndexedRowMatrix().columnSimilarities()
    val sim = similarity.entries
    sim
  }


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
//      .master("spark://master:7077")
      .master("local[*]")
      .appName("ItemBaseCfTest")
      .config("spark.executor.memory","2g")
      .config("spark.locality.wait","60")
      .getOrCreate()

//    val lines: RDD[String] = spark.sparkContext.textFile("F:\\projectP\\LearningPython3\\Recommender_system\\recommendation_system_codes\\movielens\\ml-1m\\ratings.dat")
    val lines: RDD[String] = spark.sparkContext.textFile("F:\\projectP\\Python3WithNoteBook\\Recommender_system\\recommendation_system_codes\\movielens\\ml-100k\\u.data")
//    val lines: RDD[String] = spark.sparkContext.textFile("F:\\projectP\\LearningPython3\\Recommender_system\\ratings_test.dat")
    //数据整理
    val cfData: RDD[Cfdata] = lines.map(line => {
      val fields = line.split("\t")
      val userId = fields(0).toLong
      val itemId = fields(1).toLong
      val rating = fields(2).toDouble
      Cfdata(userId, itemId, rating)
    })
    import spark.implicits._
    val cfDf: DataFrame = cfData.toDF
    val cfDs: Dataset[Cfdata] = cfData.toDS

    val Array(training, test): Array[Dataset[Cfdata]] = cfDs.randomSplit(Array(0.8, 0.2))

//    training.cache()
//    test.cache()

    val test1: CoordinateMatrix = parsetoMatrix(training.toDF)
    val itemSim: DataFrame = StandardCosine(test1).collect.toIndexedSeq.toDF("itemX", "itemY", "sim")

    training.createOrReplaceTempView("training")
    itemSim.createOrReplaceTempView("itemSim")
    test.createOrReplaceTempView("test")


    // 计算测试集相似物品表
    val testItemSim = spark.sql(
      """
        |select test.userId, test.itemId, test.rating testRating, training.rating, sim.sim
        |from test
        |left join training on test.userId = training.userId
        |left join itemSim sim on test.itemId=sim.itemX and training.itemId=sim.itemY
      """.stripMargin)

//    testItemSim.cache()
//    testItemSim.show()
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


//    val logs: DataFrame = spark.read.format("jdbc").options(
//      Map("url" -> "jdbc:mysql://localhost:3306/test",
//        "driver" -> "com.mysql.jdbc.Driver",
//        "dbtable" -> "domain_object_discern",
//        "user" -> "root",
//        "password" -> "741852")
//    ).load()



    // 往数据库里面写数据
    val props = new Properties()
    props.put("user", "root")
    props.put("password", "741852")
    // 如果表已经存在,覆盖
    testAndPre.write.mode("Overwrite")
      .jdbc("jdbc:mysql://localhost:3306/test", "item_base_cf_result",props)

    testAndPre.show()

//
//    mytest2.collect.foreach(println)

//    spark.sql(
//      """
//        |select test.userId, test.itemId, test.rating testRating,
//      """.stripMargin)

//    traing.show()

    spark.stop()

  }

}
