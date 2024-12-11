package com.metabolic.data.core.services.spark.reader

import com.metabolic.data.mapper.domain.run.EngineMode.EngineMode
import com.metabolic.data.mapper.domain.run.EngineMode
import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataframeUnifiedReader {

  val input_identifier: String

  def readBatch(spark: SparkSession): DataFrame

  def readStream(spark: SparkSession): DataFrame

  def preHook(): Unit = {}

  def postHook(df: DataFrame): DataFrame = { df }

  def read(spark: SparkSession, mode: EngineMode): DataFrame = {

    preHook()

    val df = mode match {
      case EngineMode.Batch => readBatch(spark)
      case EngineMode.Stream => readStream(spark)
    }

    postHook(df)

  }



}
