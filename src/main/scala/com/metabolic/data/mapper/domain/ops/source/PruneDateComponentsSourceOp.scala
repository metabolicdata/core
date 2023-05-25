package com.metabolic.data.mapper.domain.ops.source

import com.metabolic.data.core.services.spark.filter.DataLakeDepth.{DAY, DataLakeDepth}
import com.metabolic.data.core.services.util.DefaultsUtilsService
import com.metabolic.data.mapper.domain.ops.SourceOp
import org.joda.time.DateTime

class PruneDateComponentsSourceOp(
                      val from: String,
                      val to: String,
                      val depth: DataLakeDepth = DAY
                    ) extends SourceOp {

  val opName = "prune"

  val datetimeFormatter = DefaultsUtilsService.datetimeFormatter

  val fromDate: DateTime = datetimeFormatter.parseDateTime(from)
  val toDate: DateTime = datetimeFormatter.parseDateTime(to)

}

object PruneDateComponentsSourceOp {

  def apply(from: String, to: String, depth: DataLakeDepth): PruneDateComponentsSourceOp = {
      new PruneDateComponentsSourceOp(from, to, depth)
  }

  def apply(from: String, to: String): PruneDateComponentsSourceOp = {
    new PruneDateComponentsSourceOp(from, to)
  }

}

