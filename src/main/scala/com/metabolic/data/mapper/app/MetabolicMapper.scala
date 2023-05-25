package com.metabolic.data.mapper.app

import com.metabolic.data.core.services.spark.interval.IntervalReader
import com.metabolic.data.mapper.domain.ops.mapping.TupletIntervalMapping
import com.metabolic.data.mapper.domain.ops.{Mapping, SQLMapping}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.ParseException

object MetabolicMapper extends Logging {

  def map(spark: SparkSession, mapping: Mapping) = {

    mapping match {
      case sqlmapping: SQLMapping => {
        val sql = sqlmapping.sqlContents
        logger.info(s"Mapping SQL: $sql")

        try {
          val output = spark.sql(sql)
          output.createOrReplaceTempView("output")

        } catch {
          case pe: ParseException => {
            pe.getStackTrace
            logger.error(pe.getMessage(), pe)
            logger.error(s"Error while Parsing $sql in " + pe.getStackTrace)
          }
        }

      }
      case intervalMapping: TupletIntervalMapping => {
        val left = spark.table(intervalMapping.leftTableName)
        val right = spark.table(intervalMapping.rightTableName)

        val transform = new IntervalReader(left, intervalMapping.leftIdColumnName, intervalMapping.leftWindowColumnName)
          .generate( right, intervalMapping.rightIdColumnName, intervalMapping.rightWindowColumnName)

        transform.createOrReplaceTempView(intervalMapping.resultTableName)
      }
    }
  }

}
