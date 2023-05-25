package com.metabolic.data.core.services.util

import com.typesafe.config.Config
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object DefaultsUtilsService {

  val periodFormatter = DateTimeFormat.forPattern("yyyy-MM")
  val dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
  val datetimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss")
  //val isoDatetimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

  def getDateTime(key: String, config: Config): DateTime = {
    val dateTimeString = config.getString(key)
    datetimeFormatter.parseDateTime(dateTimeString)
  }


  def getDate(forKey: String, config: Config): DateTime = {

    val value = config.getString(forKey)
    dateFormatter.parseDateTime(value)

  }

  def getPeriod(forKey: String, config: Config): DateTime = {

    val value = config.getString(forKey)
    periodFormatter.parseDateTime(value)

  }

}
