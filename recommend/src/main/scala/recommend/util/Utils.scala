package recommend.util

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Created by wuyukai on 15/6/29.
 */
object Utils {

  /**
   * 生成当前时间字符串格式“2015-06-29/14-33-30”
   **/
  def generateDate(): String = {
    generateDate("YYYYMMdd/HH")
  }

  def generateDate(format:String):String = {
    val day = new Date()
    val simpleDate = new SimpleDateFormat(format)
    simpleDate.format(day)

  }


}
