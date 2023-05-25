package com.metabolic.data.mapper.domain.ops.source

import com.metabolic.data.core.services.util.DefaultsUtilsService
import com.metabolic.data.mapper.domain.ops.SourceOp
import org.joda.time.DateTime


class FilterSourceOp(
                      val onColumn: String,
                      private val from: String,
                      private val to: String
                    ) extends SourceOp {

  val opName = "filter"

  val datetimeFormatter = DefaultsUtilsService.datetimeFormatter

  val fromDate: DateTime = datetimeFormatter.parseDateTime(from)
  val toDate: DateTime = datetimeFormatter.parseDateTime(to)

}

object FilterSourceOp {

  def apply(onColumn: String, from: String, to: String): FilterSourceOp = {
    new FilterSourceOp(onColumn, from, to)
  }
}