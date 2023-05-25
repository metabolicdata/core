package com.metabolic.data.core.services.spark.interval

import com.metabolic.data.core.services.spark.transformations.DedupeTransform
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class IntervalReader(l: DataFrame, leftJoinColumn: String, leftWindowColumn: String) {

    val primaryWindowColumn = "__primary__ts"
    val secondaryWindowColumn = "__secondary__ts"

    def generate(r: DataFrame, rightJoinColumn: String, rightWindowColumn: String): DataFrame = {

        val left = l.join(r, l(leftJoinColumn) === r(rightJoinColumn), "left")
          .where(l(leftWindowColumn) > r(rightWindowColumn))
          .withColumn(primaryWindowColumn, l(leftWindowColumn))
          .withColumn(secondaryWindowColumn, r(rightWindowColumn))

        val right = l.join(r, l(leftJoinColumn) === r(rightJoinColumn), "right")
          .where(l(leftWindowColumn) <= r(rightWindowColumn))
          .withColumn(primaryWindowColumn, r(rightWindowColumn))
          .withColumn(secondaryWindowColumn, l(leftWindowColumn))

        val intervals = left.unionByName(right)

        intervals
          .transform(new DedupeTransform(Seq(leftJoinColumn, rightJoinColumn, primaryWindowColumn), Seq(secondaryWindowColumn)).dedupe())
          .drop(primaryWindowColumn, secondaryWindowColumn)

    }
}