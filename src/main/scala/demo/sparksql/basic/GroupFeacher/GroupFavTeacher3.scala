package demo.sparksql.basic.GroupFeacher

import java.net.URL

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object GroupFavTeacher3 {

  def main(args: Array[String]): Unit = {

    val topN = 3
    val conf = new SparkConf().setAppName("FavTeacher").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //指定以后从哪里读取数据
    val lines: RDD[String] = sc.textFile("F:\\projectJ\\data")
    //整理数据
    val subjectAndteacher: RDD[((String, String), Int)] = lines.map(line => {
      val index = line.lastIndexOf("/")
      val teacher2 = line.substring(index + 1)
      val httphost = line.substring(0, index)
      val subject = new URL(httphost).getHost.split("[.]")(0)
      ((subject, teacher2), 1)
    })

    //和一组合在一起(这种方式不好,调用了两次map方法
    //    val map: RDD[((String, String), Int)] = subjectAndteacher.map((_, 1))

    //聚合, 将学科和老师联合当作key
    val reduced: RDD[((String, String), Int)] = subjectAndteacher.reduceByKey(_ + _)

    //计算有多少学科
    val subjects: Array[String] = reduced.map(_._1._1).distinct().collect()

    val sbPatitioner = new SubjectPartitioner(subjects)

    //分区器,自定义一个分区器,并且按照指定的分区器进行排序

    //调用partitionBy的时候RDD的key是一个元祖
    val partitioned: RDD[((String, String), Int)] = reduced.partitionBy(sbPatitioner)

    //如果一次拿出一个分区(可以操作一个分区中的数据了)
    val sorted: RDD[((String, String), Int)] = partitioned.mapPartitions(it => {
      //将迭代器转换成list,然后排序,再转换成迭代器返回
      it.toList.sortBy(_._2).reverse.take(topN).iterator
    })

    //收集结果
    val result: Array[((String, String), Int)] = sorted.collect()

    println(result.toBuffer)

    sc.stop()
  }

}



// 自定义分区器
class SubjectPartitioner(sbs: Array[String]) extends Partitioner {


  // 相当与主构造器(方法只会new的时候执行一次)
  // 用于存放规则的一个app
  val rules = new mutable.HashMap[String, Int]()
  var i = 0
  for (sb <- sbs) {
//    rules(sb) = i
    rules.put(sb, i)
    i += 1
  }


  // 返回分区的数量(下一个RDD有多少分区)
  override def numPartitions: Int = sbs.length

  // 根据传入的key计算分区标号
  // key是一个元祖(String, String)
  override def getPartition(key: Any): Int = {
    //获取学科名称
    val subject = key.asInstanceOf[(String, String)]._1
    //根据规则计算分区编号
    rules(subject)
  }

}