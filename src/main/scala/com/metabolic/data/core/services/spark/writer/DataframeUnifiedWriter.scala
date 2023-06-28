package com.metabolic.data.core.services.spark.writer

import com.metabolic.data.mapper.domain.io.EngineMode
import com.metabolic.data.mapper.domain.io.EngineMode.EngineMode
import io.delta.tables.DeltaTable
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SaveMode}

trait DataframeUnifiedWriter extends Logging {

  val output_identifier: String
  val checkpointLocation: String

  val saveMode: SaveMode

  def writeBatch(df: DataFrame): Unit

  def writeStream(df: DataFrame): StreamingQuery

  def preHook(df: DataFrame): DataFrame = { df }

  def postHook(df: DataFrame, streamingQuery: Option[StreamingQuery] ): Boolean = {

//    if (DeltaTable.isDeltaTable(output_identifier)){
//      val deltaTable = DeltaTable.forPath(output_identifier)
//
//      logger.info("optimize with z order")
//      deltaTable.optimize().executeZOrderBy("column")
//
//    }
    streamingQuery.flatMap(stream => Option.apply(stream.awaitTermination()))
    true

  }

  def write(df: DataFrame, mode: EngineMode): Boolean = {

    logger.info(s"Writing $output_identifier")

    logger.info(s"Executing $output_identifier preHook")
    val _df = preHook(df)

    val streamingQuery = mode match {
      case EngineMode.Batch =>
        logger.info(s"Writing $output_identifier as BATCH")
        writeBatch(_df)
        Option.empty

      case EngineMode.Stream =>
        logger.info(s"Writing $output_identifier as Stream")
        Option.apply(writeStream(_df))
    }

    logger.info(s"Executing $output_identifier postHook")
    postHook(df, streamingQuery)

  }



}
