package machinelearning.recommend2demo.ALSCf

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

/**
  * Created by Joe.Kwan on 2018/11/22 16:07. 
  */
object ALSTest {

  /**
    * Compute the cosine similarity between two vectors
    */
  def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix):Double ={
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }



  /**
    * Compute RMSE (Root Mean Squared Error)
    * @param args
    */
  def computeRmse(model: MatrixFactorizationModel, data:RDD[Rating]) = {
    val userProducts = data.map {case Rating(user, product, rating) =>
      ((user, product))
    }

    val predictions = model.predict(userProducts).map { case Rating(user, product, rating) =>
      ((user, product),rating)
    }

    val ratesAndPreds = data.map { case Rating(user, product, rating) =>
      ((user, product),rating)
    }.join(predictions)

    math.sqrt(ratesAndPreds.map { case ((user, product), (r1, r2)) =>
    val err = (r1 - r2)
    err * err
    }.mean())
  }


  /**
    * 给某一个用户推荐前k个商品的平均准确度MAPK, 该算法定义如下
    * @param args
    */
  def avgPrecisionK(actual: Seq[Int], predicted: Seq[Int], k: Int): Double = {
    val predK = predicted.take(k)
    var score = 0.0
    var numHits = 0.0
    for ((p, i) <- predK.zipWithIndex) {
      if (actual.contains(p)) {
        numHits += 1.0
        score += numHits / (i.toDouble + 1.0)
      }
    }
    if (actual.isEmpty) {
      1.0
    } else {
      score / scala.math.min(actual.size, k).toDouble
    }
  }



  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]").appName("ALStest")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    val lines: RDD[String] = spark.sparkContext.textFile("F:\\projectP\\Python3WithNoteBook\\Recommender_system\\recommendation_system_codes\\movielens\\ml-1m\\ratings.dat")


    val ratings = lines.map(line => {
      val fields = line.split("::")
      val userId = fields(0).toInt
      val itemId = fields(1).toInt
      val rating = fields(2).toDouble
      Rating(userId, itemId, rating)
    }).cache()
    println(ratings.first)


    val users = ratings.map(_.user).distinct()
    val products = ratings.map(_.product).distinct()
//    println("Got "+ratings.count()+" ratings from "+users.count+" users on "+products.count+" products.")


    val splits = ratings.randomSplit(Array(0.8, 0.2), seed=111)
    val training = splits(0).repartition(numPartitions = 50)
    val test = splits(1).repartition(numPartitions = 50)


    // trainingModel
    val rank = 12
    val lambda = 0.01
    val numIterations = 20
    val model = ALS.train(ratings, rank, numIterations, lambda)


//    println(model.userFeatures)       // 用户和商品的特征向量
//    println(model.userFeatures.count)
//    println(model.productFeatures)
//    println(model.productFeatures.count)


    // 将训练集当作测试集来进行对比测试
    val usersProducts = ratings.map { case Rating(user, product, rating) =>
      (user, product)
    }
//    println(usersProducts.count)    // 测试集的记录等于评分总记录数


    var predictions = model.predict(usersProducts).map { case Rating(user, product, rating) =>
      ((user, product), rating)
    }
//    println(predictions.count)


    // 将真实评分数据集与预测数据集进行合并
    val ratesAndPreds = ratings.map { case Rating(user, product, rating) =>
      ((user, product), rating)
    }.join(predictions)


    // 计算均方差
    val rmse = math.sqrt(ratesAndPreds.map { case((user, product), (r1, r2)) =>
      val err = (r1 - r2)
        err * err
    }.mean())
//    println(s"RMSE = $rmse")


    // 保存真实评分和预测评分(先按照用户排序,然后重新分区确保目标目录中只生成一个文件, 如果重复运行这段代码,则需要想删除目标路径)
//    ratesAndPreds.sortByKey().repartition(1).sortBy(_._1).map({
//      case ((user, product),(rate, pred)) => (user + "," + product + "," + rate + "," + pred)
//    }).saveAsTextFile("/tmp/result")


    // 给一个用户推荐商品
//    users.take(5)     // 找出5个用户
    val userId = users.take(1)(0)
    val K = 10
    val topKRecs = model.recommendProducts(userId, K)
//    println(topKRecs.mkString("\n"))


    // 查看该用户的评分记录:
    val productsForUser = ratings.keyBy(_.user).lookup(384)
//    println(productsForUser)
//    println(productsForUser.size)      // 该用户对多少个商品评过分
//    println(productsForUser.sortBy(-_.rating).take(10).map(rating=>(rating.product, rating.rating)).foreach(println))    // 浏览的商品是哪些



    // 该用户对某一个商品的实际评分和预测评分方差为多少
    val actualRating = productsForUser.take(1)(0)
    val predictiedRating = model.predict(384, actualRating.product)
    val squaredError = math.pow(predictiedRating - actualRating.rating, 2.0)
//    println(actualRating.rating,predictiedRating, squaredError)


    // 如何找出和一个已知商品最相似的商品呢?
    val itemId = 2055
    val itemFactor = model.productFeatures.lookup(itemId).head
    val itemVector = new DoubleMatrix(itemFactor)
//    println(cosineSimilarity(itemVector, itemVector))


    // 找到和该商品最相似的10个商品
    val sims = model.productFeatures.map{ case (id, factor) =>
      val factorVector = new DoubleMatrix(factor)
      val sim = cosineSimilarity(factorVector, itemVector)
      (id, sim)
    }
    val sortedSims = sims.top(K)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity})
//    println(sortedSims.mkString("\n"))


    // 最相似的为该商品本身, 取前k+1个, 然后排除第一个
    val sortedSim2 = sims.top(K + 1)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity})
//    println(sortedSim2.slice(1, K+1).map{ case (id, sim) => (id, sim)}.mkString("\n"))


    // 给用户推荐的商品为
    val actualProducts = productsForUser.map(_.product)
    // 给用户预测的商品为
    val predictedProducts = topKRecs.map(_.product)
    val apk10 = avgPrecisionK(actualProducts, predictedProducts, 10)
//    println(actualProducts)
//    println(predictedProducts)
//    println(apk10)


    // 批量推荐-遍历内存中的一个集合然后循环调用RDD操作,运行比较慢
////    val users = ratings.map(_.user).distinct()       // 开始已经定义过
//    users.collect.flatMap { user =>
//      model.recommendProducts(user, 10)
//    }.foreach(println)


    // 批量推荐,直接操作model中的userFeatures和ProductFeatures- 不是最优方法
//    val itemFactors = model.productFeatures.map{ case(id, factor) => factor}.collect()
//    val itemMatrix = new DoubleMatrix(itemFactor)
//    println(itemMatrix.rows, itemMatrix.columns)
//    // broadcast the item factor matrix
//    val imBroadcast = spark.sparkContext.broadcast(itemMatrix)
//    // 获取商品和索引的映射
//    var idxProducts = model.productFeatures.map { case (product, factor) => product}.zipWithIndex()
//      .map{ case (product, idx) => (idx, product)}.collectAsMap()
//    val idxProductsBroadcast = spark.sparkContext.broadcast(idxProducts)
//    val allRecs = model.userFeatures.map{ case(user, array) =>
//      val userVector = new DoubleMatrix(array)
//      val scores = imBroadcast.value.mmul(userVector)
//      val sortedWithId = scores.data.zipWithIndex.sortBy(-_._1)
//        // 根据索引取对应的商品id
//      val recommendedProducts = sortedWithId.map(_._2).map{ idx=>idxProductsBroadcast.value.get(idx).get}
//      (user, recommendedProducts)
//    }


    // 优化后的方法
//    val productFeatures = model.productFeatures.collect()
//    var productArray = ArrayBuffer[Int]()
//    var productFeaturesArray = ArrayBuffer[Array[Double]]()
//    for ((product, features) <- productFeatures) {
//      productArray += product
//      productFeaturesArray += features
//    }
//    val productArrayBroadcast = spark.sparkContext.broadcast(productArray)
//    val productFeatureMatrixBroadcast = spark.sparkContext.broadcast(new DoubleMatrix(productFeaturesArray.toArray).transpose())
//    start = System.currentTimeMillis()
//    val allRecs = model.userFeatures.mapPartitions{ iter =>
//      // Build user feature matrix for jblas
//      var userFeaturesArray = ArrayBuffer[Array[Double]]()
//      var userArray = new ArrayBuffer[Int]()
//      while (iter.hasNext) {
//        val (user, features) = iter.next()
//        userArray += user
//        userFeaturesArray += features
//      }
//      var userFeatureMatrix = new DoubleMatrix(userFeaturesArray.toArray)
//      var userRecommendationMatrix = userFeatureMatrix.mmul(productFeatureMatrixBroadcast.value)
//      var productArray=productArrayBroadcast.value
//      var mappedUserRecommendationArray = new ArrayBuffer[String](params.topk)
//      // Extract ratings from the matrix
//      for (i <- 0 until userArray.length) {
//        var ratingSet =  mutable.TreeSet.empty(Ordering.fromLessThan[(Int,Double)](_._2 > _._2))
//        for (j <- 0 until productArray.length) {
//          var rating = (productArray(j), userRecommendationMatrix.get(i,j))
//          ratingSet += rating
//        }
//        mappedUserRecommendationArray += userArray(i)+","+ratingSet.take(params.topk).mkString(",")
//      }
//      mappedUserRecommendationArray.iterator
//    }



    spark.stop()

  }

}
