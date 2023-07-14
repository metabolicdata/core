package com.metabolic.data.core.services.spark.reader.file

import com.metabolic.data.core.services.spark.reader.DataframeUnifiedReader
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.io.Directory
import java.io.File

class DeltaReader(val input_identifier: String, historical: Boolean, startTimestamp: String, checkpointPath: String) extends DataframeUnifiedReader {

  import io.delta.implicits._

  override def readBatch(spark: SparkSession): DataFrame = {

    val sr = spark.read
    val osr = historical match {
      case true => {
        val directoryPath = new Directory(new File(checkpointPath))
        directoryPath.deleteRecursively()
        sr
      }
      case false => startTimestamp match {
        case "" => sr
        case _ => sr.option("timestampAsOf", startTimestamp)
      }
    }
    osr
      .delta(input_identifier)

  }

  override def readStream(spark: SparkSession): DataFrame = {

    val sr = spark.readStream
    val osr = historical match {
      case true   => {
        val directoryPath = new Directory(new File(checkpointPath))
        directoryPath.deleteRecursively()
        sr.option("startingTimestamp", "2000-01-01")
      }
      case false  => startTimestamp match {
        case "" => sr
        case _ => sr.option("startingTimestamp", startTimestamp)}
    }

    osr
        .delta(input_identifier)
  }

}

object DeltaReader {
  def apply(input_identifier: String) = new DeltaReader(input_identifier, false, "", "")
  def apply(input_identifier: String, historical: Boolean, checkpointPath: String) = new DeltaReader(input_identifier, historical, "", checkpointPath)
  def apply(input_identifier: String, startTimestamp: String) = new DeltaReader(input_identifier, false, startTimestamp, "")


}
