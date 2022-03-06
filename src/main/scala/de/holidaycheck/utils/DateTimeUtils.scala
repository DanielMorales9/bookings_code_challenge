package de.holidaycheck.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat

object DateTimeUtils {

  def parseDateTime(
      dateTimeString: String,
      dateTimeFormat: String = "yyyy-MM-dd HH:mm:ss.SSS"
  ): Timestamp = {
    val dateFormat = new SimpleDateFormat(dateTimeFormat)
    val parsedDate = dateFormat.parse(dateTimeString)
    new Timestamp(parsedDate.getTime)
  }
}
