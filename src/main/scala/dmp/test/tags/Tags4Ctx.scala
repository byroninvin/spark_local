//package dmp.test.tags
//
//
//
//import dmp.test.utils.TagsUtils
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.{SparkConf, SparkContext}
//
//
///**
//  * Created by Joe.Kwan on 2018/9/4.
//  */
//object Tags4Ctx {
//
//  /**
//    * 用户当天的标签
//    *
//    *
//    *
//    */
//
//  def main(args: Array[String]): Unit = {
//
//
//    if (args.length != 4) {
//      println(
//        """
//          |dmp.test.tags.Tags4Ctx
//          |参数
//          | 输入路径
//          | 字典文件路径
//          | stopWordsFilePath
//          | 输出路径
//        """.stripMargin
//      )
//      sys.exit()
//    }
//
//    val Array(inputPath, dictFilePath, stopWordsFilePath,outputPath) = args
//
//    // 2
//    val sparkConf = new SparkConf()
//    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
//    sparkConf.setMaster("local[*]")
//    //
//    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//
//    val sc = new SparkContext(sparkConf)
//    //
//    val sQLContext = new SQLContext(sc)
//
//
//    // dict----app
//    val dictMap = sc.textFile(dictFilePath).map(line => {
//      val fields = line.split("\t", -1)
//      (fields(4), fields(1))
//    }).collect().toMap
//
//    // dict----stopWords
//    val stopWordsMap = sc.textFile(stopWordsFilePath).map((_, 0)).collect().toMap
//
//    // broadcast-> executor
//    val broadcastAppDict = sc.broadcast(dictMap)
//    val broadcastStopWordsDict = sc.broadcast(stopWordsMap)
//
//    // loading parquet
//    import sQLContext.implicits._
//    sQLContext.read.parquet(inputPath).where(TagsUtils.hasSomeUserIdCondition).map(row => {
//      // 行数据进行标签化处理
//      val ads = Tags4Ads.makeTags(row)
//      val apps = Tags4App.makeTags(row, broadcastAppDict.value)
//      val devices = Tags4Devices(row)
//      val keyWords = Tags4KeyWords.makeTags(row, broadcastStopWordsDict.value)
//      val allUserId = TagsUtils.getAllUserId(row)
//
//      (allUserId(0), (ads ++ apps ++ devices ++ keyWords).toList)
//
//    }).rdd.reduceByKey((a, b) => {
//      // (a ++ b).groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2)).toList
//
//      (a ++ b).groupBy(_._1).map {
//        case(k, sameTags) => (k, sameTags.map(_._2).sum)
//      }.toList
//
//    })
//
//    sc.stop()
//
//  }
//
//}
