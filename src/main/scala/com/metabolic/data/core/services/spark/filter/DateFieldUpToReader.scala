package com.metabolic.data.core.services.spark.filter

import org.apache.logging.log4j.scala.Logging

import org.joda.time.DateTime

class DateFieldUpToReader(val dateColumn: String,
                          val dateValue: DateTime)
  extends DataframeFilter with Logging {

  val statement: String = {

    logger.info(s"Filtering up to $dateValue ($dateColumn <= '$dateValue')")

    s"$dateColumn <= '$dateValue'"
  }
}
