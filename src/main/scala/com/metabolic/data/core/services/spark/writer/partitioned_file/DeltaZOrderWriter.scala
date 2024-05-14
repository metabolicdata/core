package com.metabolic.data.core.services.spark.writer.partitioned_file

import com.amazonaws.regions.Regions
import com.metabolic.data.core.services.spark.writer.DataframePartitionWriter
import com.metabolic.data.core.services.spark.writer.file.DeltaWriter
import com.metabolic.data.mapper.domain.io.WriteMode.WriteMode
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

class DeltaZOrderWriter(val partitionColumnNames: Seq[String],
                        outputPath: String,
                        writeMode: WriteMode,
                        dateColumnName: Option[String],
                        idColumnName: Option[String],
                        dbName: String,
                        override val checkpointLocation: String,
                        namespaces: Seq[String],
                        optimize: Option[Boolean] = None, optimizeEvery: Option[Int] = None, retention: Double = 168d)(implicit regions: Regions, spark: SparkSession)

  extends DeltaWriter(outputPath, writeMode, dateColumnName, idColumnName, checkpointLocation, dbName, namespaces, optimize, optimizeEvery)
    with DataframePartitionWriter {

  override val output_identifier: String = outputPath

  override def compactAndVacuum(): Unit = {
    logger.info(s"Compacting Delta table $outputPath with columns $partitionColumnNames")
    val deltaTable = DeltaTable.forPath(outputPath)
    deltaTable.optimize().executeZOrderBy(partitionColumnNames: _*)
    logger.info(s"Vacumming Delta table $outputPath with retention $retention")
    deltaTable.vacuum(retention)
  }


}



