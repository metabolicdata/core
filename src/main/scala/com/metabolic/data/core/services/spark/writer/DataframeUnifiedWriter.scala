package com.metabolic.data.core.services.spark.writer

import com.metabolic.data.mapper.domain.io.EngineMode
import com.metabolic.data.mapper.domain.io.EngineMode.EngineMode
import com.metabolic.data.mapper.domain.io.WriteMode.WriteMode
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.DataFrame

trait DataframeUnifiedWriter extends Logging {

  val output_identifier: String
  val checkpointLocation: String

  val writeMode: WriteMode

  def writeBatch(df: DataFrame): Unit

  def writeStream(df: DataFrame): StreamingQuery

  def preHook(df: DataFrame): DataFrame = { df }

  def postHook(df: DataFrame, streamingQuery: Seq[StreamingQuery] ): Unit = {}

  def write(df: DataFrame, mode: EngineMode): Seq[StreamingQuery] = {

    logger.info(s"Writing $output_identifier")

    logger.info(s"Executing $output_identifier preHook")
    val _df = preHook(df)

    val streamingQueries = mode match {
      case EngineMode.Batch =>
        logger.info(s"Writing $output_identifier as BATCH")
        writeBatch(_df)
        Seq.empty

      case EngineMode.Stream =>
        logger.info(s"Writing $output_identifier as Stream")
        val query = writeStream(_df)
        Seq(query)
    }

    logger.info(s"Executing $output_identifier postHook")
    postHook(df, streamingQueries)
    streamingQueries

  }

}
