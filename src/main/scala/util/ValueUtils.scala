package util

import com.typesafe.config.ConfigFactory
import org.apache.commons.lang3.StringUtils

/**
  * Created by Kerven-HAN on 2019/8/14 21:20.
  * Talk is cheap , show me the code 
  */
object ValueUtils {

  val load = ConfigFactory.load()

  def getStringValue(key: String, defaultValue: String = "") = {
    val value = load.getString(key)
    if (StringUtils.isNotEmpty(value)) {
      value
    } else {
      defaultValue
    }
  }

}
