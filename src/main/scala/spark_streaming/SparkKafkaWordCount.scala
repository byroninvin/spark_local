package spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by Joe.Kwan on 2020/6/3
 */
object SparkKafkaWordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    // 创建DStream, 需要KafkaStream,不建议使用高级的api
    val zkQuorum = "node-1.xiaoniu.com:2181,node-2.xiaoniu.com:2081"
    val groupId = "g1"
    val topic = Map[String, Int]("niu123" -> 1)  // 线程

    // 创建DStream，需要KafkaDStream
    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topic)

    // 对数据进行处理
    // kafka的ReceiverInputDSteam[(String, String)]里面装的是一个元祖（key是写入的key， value是实际写入的内容）
    val lines: DStream[String] = data.map(_._2)
    
    // 对DStream进行操作，操作这个操作（代理）描述，就像操作一个本地的集合一样
    val words: DStream[String] = lines.flatMap(_.split(" "))
    // 单词和一组合在一起
    val wordAndOne: DStream[(String, Int)] = words.map((_, 1))
    // 聚合
    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    // 打印结果（Action)
    reduced.print()
    // 启动SparkStreaming程序
    ssc.start()
    // 等待优雅的退出
    ssc.awaitTermination()

  }

}
