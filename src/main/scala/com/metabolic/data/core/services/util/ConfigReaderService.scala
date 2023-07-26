package com.metabolic.data.core.services.util

import com.amazonaws.regions.Regions
import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.{DateTime, DateTimeZone}

class ConfigReaderService(implicit val region: Regions) {

  def preloadConfig(utcTime: DateTime = DateTime.now().withZone(DateTimeZone.UTC)): Map[String, String] = {

    val datetimeFormatter = DefaultsUtilsService.datetimeFormatter
    val dateFormatter = DefaultsUtilsService.dateFormatter
    val periodFormatter = DefaultsUtilsService.periodFormatter

    Map(
      "df.now" -> s"${utcTime.toString(datetimeFormatter)}",

      "df.one_hour_ago" -> s"${utcTime.minusHours(1).toString(datetimeFormatter)}",
      "df.two_hours_ago" -> s"${utcTime.minusHours(2).toString(datetimeFormatter)}",
      "df.three_hours_ago" -> s"${utcTime.minusHours(3).toString(datetimeFormatter)}",
      "df.four_hours_ago" -> s"${utcTime.minusHours(4).toString(datetimeFormatter)}",
      "df.five_hours_ago" -> s"${utcTime.minusHours(5).toString(datetimeFormatter)}",
      "df.six_hours_ago" -> s"${utcTime.minusHours(6).toString(datetimeFormatter)}",
      "df.seven_hours_ago" -> s"${utcTime.minusHours(7).toString(datetimeFormatter)}",
      "df.eight_hours_ago" -> s"${utcTime.minusHours(8).toString(datetimeFormatter)}",
      "df.nine_hours_ago" -> s"${utcTime.minusHours(9).toString(datetimeFormatter)}",
      "df.ten_hours_ago" -> s"${utcTime.minusHours(10).toString(datetimeFormatter)}",
      "df.eleven_hours_ago" -> s"${utcTime.minusHours(11).toString(datetimeFormatter)}",

      "df.today" -> s"${utcTime.toString()}",
      "df.start_of_today" -> s"${utcTime.withMillisOfDay(0).toString(datetimeFormatter)}",
      "df.end_of_today" -> s"${utcTime.withMillisOfDay(0).plusDays(1).plusMillis(-1).toString(datetimeFormatter)}",

      "df.tomorrow" -> s"${utcTime.plusDays(1).toString(dateFormatter)}",
      "df.start_of_tomorrow" -> s"${utcTime.plusDays(1).withMillisOfDay(0).toString(datetimeFormatter)}",
      "df.end_of_tomorrow" -> s"${utcTime.plusDays(2).withMillisOfDay(0).plusMillis(-1).toString(datetimeFormatter)}",

      "df.yesterday" -> s"${utcTime.plusDays(-1).toString(dateFormatter)}",
      "df.start_of_yesterday" -> s"${utcTime.plusDays(-1).withMillisOfDay(0).toString(datetimeFormatter)}",
      "df.end_of_yesterday" -> s"${utcTime.withMillisOfDay(0).plusMillis(-1).toString(datetimeFormatter)}",

      "df.month" -> s"${utcTime.getMonthOfYear}",
      "df.year_month" -> s"${utcTime.toString(periodFormatter)}",
      "df.first_day_of_month" -> s"${utcTime.withDayOfMonth(1).toString(dateFormatter)}",
      "df.last_day_of_month" -> s"${utcTime.withDayOfMonth(1).plusMonths(1).withMillisOfDay(0).plusMillis(-1).toString(dateFormatter)}",
      "df.start_of_month" -> s"${utcTime.withDayOfMonth(1).withMillisOfDay(0).toString(datetimeFormatter)}",
      "df.end_of_month" -> s"${utcTime.withDayOfMonth(1).plusMonths(1).withMillisOfDay(0).plusMillis(-1).toString(datetimeFormatter)}",

      "df.previous_month" -> s"${utcTime.plusMonths(-1).getMonthOfYear}",
      "df.previous_year_month" -> s"${utcTime.plusMonths(-1).toString(periodFormatter)}",
      "df.previous_first_day_of_month" -> s"${utcTime.plusMonths(-1).withDayOfMonth(1).toString(dateFormatter)}",
      "df.previous_last_day_of_month" -> s"${utcTime.withDayOfMonth(1).withMillisOfDay(0).plusMillis(-1).toString(dateFormatter)}",
      "df.previous_start_of_month" -> s"${utcTime.plusMonths(-1).withDayOfMonth(1).withMillisOfDay(0).toString(datetimeFormatter)}",
      "df.previous_end_of_month" -> s"${utcTime.withDayOfMonth(1).withMillisOfDay(0).plusMillis(-1).toString(datetimeFormatter)}",

      "df.next_month" -> s"${utcTime.plusMonths(1).getMonthOfYear}",
      "df.next_year_month" -> s"${utcTime.plusMonths(1).toString(periodFormatter)}",
      "df.next_first_day_of_month" -> s"${utcTime.plusMonths(1).withDayOfMonth(1).toString(dateFormatter)}",
      "df.next_last_day_of_month" -> s"${utcTime.plusMonths(2).withDayOfMonth(1).plusDays(-1).toString(dateFormatter)}",
      "df.next_start_of_month" -> s"${utcTime.plusMonths(1).withDayOfMonth(1).withMillisOfDay(0).toString(datetimeFormatter)}",
      "df.next_end_of_month" -> s"${utcTime.plusMonths(2).withDayOfMonth(1).withMillisOfDay(0).plusMillis(-1).toString(datetimeFormatter)}",

      "df.year" -> s"${utcTime.getYear}",
      "df.first_day_of_year" -> s"${utcTime.withDayOfYear(1).toString(dateFormatter)}",
      "df.last_day_of_year" -> s"${utcTime.withDayOfYear(1).plusYears(1).plusDays(-1).toString(dateFormatter)}",
      "df.start_of_year" -> s"${utcTime.withDayOfYear(1).withMillisOfDay(0).toString(datetimeFormatter)}",
      "df.end_of_year" -> s"${utcTime.withDayOfYear(1).withMillisOfDay(0).plusYears(1).plusMillis(-1).toString(datetimeFormatter)}",

      "df.previous_year" -> s"${utcTime.plusYears(-1).getYear}",
      "df.previous_first_day_of_year" -> s"${utcTime.plusYears(-1).withDayOfYear(1).toString(dateFormatter)}",
      "df.previous_last_day_of_year" -> s"${utcTime.withDayOfYear(1).plusDays(-1).toString(dateFormatter)}",
      "df.previous_start_of_year" -> s"${utcTime.plusYears(-1).withDayOfYear(1).withMillisOfDay(0).toString(datetimeFormatter)}",
      "df.previous_end_of_year" -> s"${utcTime.withDayOfYear(1).withMillisOfDay(0).plusMillis(-1).toString(datetimeFormatter)}",

      "df.next_year" -> s"${utcTime.plusYears(1).getYear}",
      "df.next_first_day_of_year" -> s"${utcTime.plusYears(1).withDayOfYear(1).toString(dateFormatter)}",
      "df.next_last_day_of_year" -> s"${utcTime.plusYears(2).withDayOfYear(1).plusDays(-1).toString(dateFormatter)}",
      "df.next_start_of_year" -> s"${utcTime.plusYears(1).withDayOfYear(1).withMillisOfDay(0).toString(datetimeFormatter)}",
      "df.next_end_of_year" -> s"${utcTime.plusYears(2).withDayOfYear(1).withMillisOfDay(0).plusMillis(-1).toString(datetimeFormatter)}"

    )

  }

  private def prepareConfig(params: Map[String, String]) = {

    params
      .filter { kv =>
        kv._1.startsWith("dp.")
      }.map { kv =>
        System.setProperty(kv._1, kv._2)
      }

    preloadConfig()
      .map { kv =>
        System.setProperty(kv._1, kv._2)
      }

    ConfigFactory.invalidateCaches()

  }

  private def loadConfig(mappingConfig: Config, environment: String): Config = {

    val baseConfig = if (environment.isEmpty) {
      ConfigFactory.load()
    } else {
      ConfigFactory.load(environment)
    }

    mappingConfig
      .withFallback(baseConfig)
      .resolve

  }

  def loadConfig(resource: String, environment: String): Config = {

    loadConfig(ConfigFactory.parseString(resource), environment)

  }

  def getConfig(configPath: String, params: Map[String, String] = Map.empty): Config = {

    prepareConfig(params)

    val content: String = new FileReaderService()
      .getFileContents(configPath)

    loadConfig(content, "")

  }

}
