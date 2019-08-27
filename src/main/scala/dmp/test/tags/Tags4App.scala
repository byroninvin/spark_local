package dmp.test.tags

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row


/**
  * Created by Joe.Kwan on 2018/9/4. 
  */
object Tags4App extends Tags {

  override def makeTags(args: Any*): Map[String, Int] = {

    var map = Map[String, Int]()
    val row = args(0).asInstanceOf[Row]

    // 广播过来的字典
    val appDict: Map[String, String] = args(1).asInstanceOf[Map[String, String]]


    val appId: String = row.getAs[String]("appid")
    val appName: String = row.getAs[String]("appname")

    if (StringUtils.isEmpty(appName)) {
      appDict.contains(appId) match {
        case true => map += "APP" + appDict.get(appId) -> 1
      }
    }


    // 渠道的
    val channelId: Int = row.getAs[Int]("adplatformproviderid")
    if (channelId > 0) map += (("CN" + channelId, 1))


    map

  }

}
