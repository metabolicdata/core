package com.metabolic.data.core.services.spark.writer.partitioned_file

import com.amazonaws.regions.Regions
import com.metabolic.data.core.services.spark.writer.DataframePartitionWriter
import com.metabolic.data.core.services.spark.writer.file.DeltaWriter
import com.metabolic.data.mapper.domain.io.WriteMode.WriteMode
import io.delta.tables.DeltaTable
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class DeltaZOrderWriter(val partitionColumnNames: Seq[String],
                        outputPath: String,
                        writeMode: WriteMode,
                        dateColumnName: Option[String],
                        idColumnName: Option[String],
                        dbName: String,
                        override val checkpointLocation: String,
                        namespaces: Seq[String])(implicit regions: Regions, spark: SparkSession)

  extends DeltaWriter(outputPath, writeMode, dateColumnName, idColumnName, checkpointLocation, dbName, namespaces)
    with DataframePartitionWriter {

  override val output_identifier: String = outputPath

  override def postHook(df: DataFrame, query: Option[StreamingQuery]): Boolean = {

    query match {
      case stream: StreamingQuery =>
        stream.awaitTermination()
      case None =>
        val deltaTable = DeltaTable.forPath(outputPath)
        deltaTable.optimize().executeZOrderBy(partitionColumnNames:_*)
        deltaTable.vacuum(retention)
    }

    true
  }

}



