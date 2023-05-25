package com.metabolic.data.core.services.spark.filter

import DataLakeDepth.DataLakeDepth
import org.apache.logging.log4j.scala.Logging

import org.joda.time.DateTime

class DateComponentsFromReader(val utcTime: DateTime,
                               val depth: DataLakeDepth)
  extends DataframeFilter with Logging {

  val statement: String = {
    

    val statement = depth match {
      case DataLakeDepth.YEAR =>
        s"yyyy >= ${utcTime.getYear}"
      case DataLakeDepth.MONTH =>
        s"yyyy > ${utcTime.getYear} OR" +
          s" ( yyyy = ${utcTime.getYear} AND mm >= ${utcTime.getMonthOfYear})"
      case DataLakeDepth.DAY =>
        s"yyyy > ${utcTime.getYear} OR" +
          s" ( yyyy = ${utcTime.getYear} AND mm > ${utcTime.getMonthOfYear}) OR" +
          s" ( yyyy = ${utcTime.getYear} AND mm = ${utcTime.getMonthOfYear} AND dd >= ${utcTime.getDayOfMonth})"
      case DataLakeDepth.HOUR =>
        s"yyyy > ${utcTime.getYear} OR" +
          s" ( yyyy = ${utcTime.getYear} AND mm > ${utcTime.getMonthOfYear}) OR" +
          s" ( yyyy = ${utcTime.getYear} AND mm = ${utcTime.getMonthOfYear} AND dd > ${utcTime.getDayOfMonth}) OR " +
          s" ( yyyy = ${utcTime.getYear} AND mm = ${utcTime.getMonthOfYear} AND dd = ${utcTime.getDayOfMonth} AND hh >= ${utcTime.getHourOfDay})"
      case DataLakeDepth.MINUTE =>
        s"yyyy > ${utcTime.getYear} OR" +
          s" ( yyyy = ${utcTime.getYear} AND mm > ${utcTime.getMonthOfYear}) OR" +
          s" ( yyyy = ${utcTime.getYear} AND mm = ${utcTime.getMonthOfYear} AND dd > ${utcTime.getDayOfMonth}) OR " +
          s" ( yyyy = ${utcTime.getYear} AND mm = ${utcTime.getMonthOfYear} AND dd = ${utcTime.getDayOfMonth} AND hh > ${utcTime.getHourOfDay}) OR" +
          s" ( yyyy = ${utcTime.getYear} AND mm = ${utcTime.getMonthOfYear} AND dd = ${utcTime.getDayOfMonth} AND hh = ${utcTime.getHourOfDay} AND mi >= ${utcTime.getMinuteOfDay})"}

    logger.info(s"Filtering from $utcTime using components with depth $depth ($statement)")
        statement

  }

  private def filterMinuteStatement(utcTime: DateTime) = {
    s"${filterHourStatement(utcTime)} OR mi >= ${utcTime.getMinuteOfDay}"
  }

  private def filterHourStatement(utcTime: DateTime) = {
    s"${filterDayStatement(utcTime)} OR hh>=${utcTime.getHourOfDay}"
  }

  private def filterDayStatement(utcTime: DateTime) = {
    s"${filterMonthStatement(utcTime)} OR dd>=${utcTime.getDayOfMonth}"
  }

  private def filterMonthStatement(utcTime: DateTime) = {
    s"${filterYearStatement(utcTime)} OR mm>=${utcTime.getMonthOfYear}"
  }

  private def filterYearStatement(utcTime: DateTime) = {
    s"yyyy>=${utcTime.getYear}"
  }
}
