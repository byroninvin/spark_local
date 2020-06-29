package timetest

import java.util.Date

import coocaa.test.common.DateUtil

/**
 * Created by Joe.Kwan on 2020/6/12
 */
object DayFormatConvert {

  def main(args: Array[String]): Unit = {

    val test: Date = DateUtil.getFormatDate("2020-05-10", "yyyyMMdd")
    val test2: String = DateUtil.getDateStr(test, "yyyyMMdd")

    val day = "2020-06-12"
    val day_String = DateUtil.getDateStr(DateUtil.getFormatDate(day), "yyyyMM")
    println(day_String)
  }

}
