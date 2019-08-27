package dmp.test.tags

import org.apache.spark.sql.Row

/**
  * Created by Joe.Kwan on 2018/9/4. 
  */
object Tags4KeyWords extends Tags {

  override def makeTags(args: Any*): Map[String, Int] = {


    var map = Map[String, Int]()
    val row = args(0).asInstanceOf[Row]

    val stopWords: Map[String, Int] = args(1).asInstanceOf[Map[String, Int]]

    val kws: String = row.getAs[String]("keywords")


    kws.split("\\|")
      .filter(kw => kw.trim.length >= 3 && kw.trim.length <= 8 && !stopWords.contains(kw.trim))
      .foreach(kw => map += "K" + kw -> 1)


    map

  }

}
