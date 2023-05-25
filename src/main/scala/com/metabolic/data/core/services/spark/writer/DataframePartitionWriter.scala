package com.metabolic.data.core.services.spark.writer

import org.apache.spark.sql.DataFrame


trait DataframePartitionWriter extends DataframeUnifiedWriter {

  val partitionColumnNames: Seq[String]

}
