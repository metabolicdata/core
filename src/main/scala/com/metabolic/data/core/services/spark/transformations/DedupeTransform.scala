package com.metabolic.data.core.services.spark.transformations

import com.metabolic.data.mapper.domain.ops.source.SQLOrder.SQLOrder
import com.metabolic.data.mapper.domain.ops.source.SQLOrder
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.DataFrame

class DedupeTransform( val dedupeColumns: Seq[String],
                       val orderColumns: Seq[String],
                       val order: SQLOrder = SQLOrder.Descending)
 extends Logging {

   def dedupe(): DataFrame => DataFrame = { df =>

     val partitionByStatement = dedupeColumns
       .mkString(",")

     val orderByStatement = orderColumns
       .map { col => s" $col $order " }
       .mkString(",")

       logger.info(s"Deduping by $partitionByStatement ordered by $orderByStatement")

       df
        .selectExpr("*", s"ROW_NUMBER() OVER(PARTITION BY $partitionByStatement ORDER BY $orderByStatement) AS rn")
        .filter("rn = 1")
        .drop("rn")

  }

}
