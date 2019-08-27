package dmp.test.utils

import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

/**
  * Created by Joe.Kwan on 2018/9/4. 
  */
object TagsUtils {

  val hasSomeUserIdCondition =
    """
      |imei != "" or imeimd5 != "" or imeisha1 != "" or
      |idfa != "" or idfamd5 != "" or idfasha1 != "" or
      |mac != "" or macmd5 != "" or macsha1 != "" or
      |androidid != "" or androididmd5 != "" or androididsha1 != "" or
      |openudid != "" or openudidmd5 != "" or openudidsha1 != ""
    """.stripMargin


  def getAllUserId(v: Row): ListBuffer[String] = {

    val userIds = new collection.mutable.ListBuffer[String]()

    if (v.getAs[String]("imei").nonEmpty) userIds.append("IM:" + v.getAs[String]("imei").toUpperCase)
    if (v.getAs[String]("idfa").nonEmpty) userIds.append("DF:" + v.getAs[String]("idfa").toUpperCase)
    if (v.getAs[String]("mac").nonEmpty) userIds.append("MC:" + v.getAs[String]("mac").toUpperCase)
    if (v.getAs[String]("androidid").nonEmpty) userIds.append("AD:" + v.getAs[String]("androidid").toUpperCase)
    if (v.getAs[String]("openudid").nonEmpty) userIds.append("OU:" + v.getAs[String]("openudid").toUpperCase)
    if (v.getAs[String]("imeimd5").nonEmpty) userIds.append("IMM:" + v.getAs[String]("imeimd5").toUpperCase)
    if (v.getAs[String]("idfamd5").nonEmpty) userIds.append("DFM:" + v.getAs[String]("idfamd5").toUpperCase)
    if (v.getAs[String]("macmd5").nonEmpty) userIds.append("MCM:" + v.getAs[String]("macmd5").toUpperCase)
    if (v.getAs[String]("androididmd5").nonEmpty) userIds.append("ADM:" + v.getAs[String]("androididmd5").toUpperCase)
    if (v.getAs[String]("openudidmd5").nonEmpty) userIds.append("OUM:" + v.getAs[String]("openudidmd5").toUpperCase)
    if (v.getAs[String]("imeisha1").nonEmpty) userIds.append("IMS:" + v.getAs[String]("imeisha1").toUpperCase)
    if (v.getAs[String]("idfasha1").nonEmpty) userIds.append("DFS:" + v.getAs[String]("idfasha1").toUpperCase)
    if (v.getAs[String]("macsha1").nonEmpty) userIds.append("MCS:" + v.getAs[String]("macsha1").toUpperCase)
    if (v.getAs[String]("androididsha1").nonEmpty) userIds.append("ADS:" + v.getAs[String]("androididsha1").toUpperCase)
    if (v.getAs[String]("openudidsha1").nonEmpty) userIds.append("OUS:" + v.getAs[String]("openudidsha1").toUpperCase)

    userIds
  }

//  def getUserId(log: Log) = {
//    log match {
//      case v if StringUtils.isNotEmpty(v.imei) => "IMEI#" + log.imei.toUpperCase
//      case v if StringUtils.isNotEmpty(v.mac) => "MAC#" + log.mac.toUpperCase
//      case v if StringUtils.isNotEmpty(v.idfa) => "IDFA" + log.idfa.toUpperCase
//      case v if StringUtils.isNotEmpty(v.androidid) => "AD" + log.androidid.toUpperCase
//      case v if StringUtils.isNotEmpty(v.openudid) => "OU" + log.openudid.toUpperCase
//
//      case v if StringUtils.isNotEmpty(v.imeimd5) => "IMM" + log.imeimd5.toUpperCase
//      case v if StringUtils.isNotEmpty(v.macmd5) => "MACM" + log.macmd5.toUpperCase
//      case v if StringUtils.isNotEmpty(v.idfamd5) => "IDFAM" + log.idfamd5.toUpperCase
//      case v if StringUtils.isNotEmpty(v.androididmd5) => "ADM" + log.androididmd5.toUpperCase
//      case v if StringUtils.isNotEmpty(v.openudidmd5) => "OUM" + log.openudidmd5.toUpperCase
//
//      case v if StringUtils.isNotEmpty(v.imeisha1) => "IMS" + log.imeisha1.toUpperCase
//      case v if StringUtils.isNotEmpty(v.macsha1) => "MACS" + log.macsha1.toUpperCase
//      case v if StringUtils.isNotEmpty(v.idfasha1) => "IDFAS" + log.idfasha1.toUpperCase
//      case v if StringUtils.isNotEmpty(v.androididsha1) => "ADS" + log.androididsha1.toUpperCase
//      case v if StringUtils.isNotEmpty(v.openudidsha1) => "OUS" + log.openudidsha1.toUpperCase
//
//      case _ =>""
//    }

}
