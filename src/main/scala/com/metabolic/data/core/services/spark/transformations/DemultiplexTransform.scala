package com.metabolic.data.core.services.spark.transformations

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.date_trunc

class DemultiplexTransform(idColumns: Seq[String], val orderColumns: Seq[String], val tsColumn: String, val format: String = "Month") extends Logging {

  val truncatedTsColumnName = "truncatedTsColumn"
  val simpleDeduper = new DedupeTransform(idColumns++Seq(truncatedTsColumnName), orderColumns)

  def generatePeriods(spark: SparkSession, fromDate: String, toDate: Option[String], format: String, endOfMonth:Boolean): DataFrame = {

      val endStatement = toDate match {
        case None => "now()"
        case Some(s) => s"to_date('$s')"
      }

    spark.sql(
       s"""SELECT
        ${if(endOfMonth && format == "Month") "last_day(date)" else "date" } AS period
        FROM
        (SELECT EXPLODE(
          (SELECT sequence(date_trunc( '$format', to_date('$fromDate')),
                           date_trunc('$format', $endStatement),
                           interval 1 ${format.toLowerCase}) AS dates)
        ) AS date)
        WHERE date <= now()"""
      )

  }

  def demultiplex(fromDate: String, toDate: Option[String], endOfMonth: Option[Boolean] = Some(false)): DataFrame => DataFrame = { df =>

    val useEndOfMonth = endOfMonth match{
      case None => false
      case Some(b) => b
    }

    val tranformedDf = df
      .withColumn(truncatedTsColumnName, date_trunc(format, df(tsColumn)))

    val simpleDeduper = new DedupeTransform(idColumns++Seq(truncatedTsColumnName), orderColumns)

    val deduped = tranformedDf.transform(simpleDeduper.dedupe())

    val periods = generatePeriods(df.sparkSession, fromDate, toDate, format, useEndOfMonth)

    val crossed = deduped.join(periods, Seq.empty, "cross")

    val filtered = crossed.where(s"period >= $truncatedTsColumnName")

    val complexDeduper = new DedupeTransform(idColumns++Seq("period"), Seq(truncatedTsColumnName))

    filtered.transform(complexDeduper.dedupe())
      .drop(truncatedTsColumnName)

  }

}
