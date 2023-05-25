package com.metabolic.data.core.services.spark.partitioner

import com.metabolic.data.core.services.schema.SchemaService
import org.apache.spark.sql.{DataFrame, SparkSession}

class SchemaManagerPartitioner(val dbName: String, val tableName: String)(implicit spark: SparkSession)
extends PartitionBuilder {

  val partitionColumnNames: Seq[String] = Seq("version")

   def addPartition(): DataFrame => DataFrame = { df =>

    new SchemaService()
      .compareSchema(dbName, tableName, df)
  }

}
