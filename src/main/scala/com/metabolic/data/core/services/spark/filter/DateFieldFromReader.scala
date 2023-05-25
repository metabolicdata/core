package com.metabolic.data.core.services.spark.filter

import org.apache.logging.log4j.scala.Logging
import org.joda.time.DateTime

class DateFieldFromReader(val dateColumn: String,
                          val dateValue: DateTime)
  extends DataframeFilter with Logging {

  def statement: String = {

    logger.info(s"Filtering from $dateValue ($dateColumn >= '$dateValue')")

    s"$dateColumn >= '$dateValue'"
  }

}
