package com.metabolic.data.core.services.spark.partitioner
import org.apache.spark.sql.DataFrame

class ExplicitPartitioner (val explicitPartitionColumnNames: Seq[String])
  extends PartitionBuilder {

  val partitionColumnNames: Seq[String] = explicitPartitionColumnNames

  override def addPartition(): DataFrame => DataFrame = { df => df }
}
