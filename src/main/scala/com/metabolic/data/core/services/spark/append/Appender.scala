package com.metabolic.data.core.services.spark.append

import com.metabolic.data.core.services.spark.reader.DataframeUnifiedReader
import com.metabolic.data.mapper.domain.run.EngineMode.EngineMode
import org.apache.spark.sql.{DataFrame, SparkSession}

object Appender {

  private def antijoin(newDf: DataFrame, idColumns: Seq[String], existingDf: DataFrame): DataFrame = {

    newDf.join(existingDf, idColumns, "leftanti")

  }

  def prepare(newDf: DataFrame, idColumns: Seq[String], reader: DataframeUnifiedReader, spark: SparkSession, mode: EngineMode): DataFrame = {

    val existingDf = reader.read(spark, mode)
    antijoin(newDf, idColumns, existingDf)

  }

}
