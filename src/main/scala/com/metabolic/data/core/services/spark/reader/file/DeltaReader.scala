package com.metabolic.data.core.services.spark.reader.file

import com.metabolic.data.core.services.spark.reader.DataframeUnifiedReader
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.io.Directory
import java.io.File

class DeltaReader(val input_identifier: String, historical: Boolean, startTimestamp: String, checkpointPath: String) extends DataframeUnifiedReader {

  import io.delta.implicits._

  override def readBatch(spark: SparkSession): DataFrame = {

    spark.read
      .option("timestampAsOf", startTimestamp)
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
      case false  => sr.option("startingTimestamp", startTimestamp)
    }

    osr
        .delta(input_identifier)
  }

}

object DeltaReader {
  def apply(input_identifier: String) = new DeltaReader(input_identifier, false, "", "")
  def apply(input_identifier: String, historical: Boolean) = new DeltaReader(input_identifier, historical, "", "")
  def apply(input_identifier: String, startTimestamp: String) = new DeltaReader(input_identifier, false, "", "")


}
