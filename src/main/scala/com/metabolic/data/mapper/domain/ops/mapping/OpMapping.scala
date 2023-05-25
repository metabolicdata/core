package com.metabolic.data.mapper.domain.ops.mapping

import com.metabolic.data.mapper.domain.ops.Mapping

trait OpMapping extends Mapping {
    val opName: String
}

trait IntervalMapping extends OpMapping {

    val opName: String = "interval"

    val leftTableName: String
    val rightTableName: String
    val leftIdColumnName: String
    val rightIdColumnName: String
    val leftWindowColumnName: String
    val rightWindowColumnName: String
    val resultTableName: String
}

case class TupletIntervalMapping(leftTableName: String,
                                 rightTableName: String,
                                 leftIdColumnName: String,
                                 rightIdColumnName: String,
                                 leftWindowColumnName: String,
                                 rightWindowColumnName: String,
                                 resultTableName: String)
  extends IntervalMapping