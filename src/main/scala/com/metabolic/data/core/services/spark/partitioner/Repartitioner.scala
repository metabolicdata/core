package com.metabolic.data.core.services.spark.partitioner

import org.apache.spark.sql.DataFrame


case class Repartitioner(partitionColumnNames: Seq[String],
                         builders: Seq[PartitionBuilder]) {

  val DEFAULT_FILES_PER_PARTITION = 2

  def repartition(): DataFrame => DataFrame = { df =>

    val _df = builders.foldLeft(df) { (df: DataFrame, builder: PartitionBuilder) =>

      df.transform(builder.addPartition())

    }

    val partitionColumns = partitionColumnNames.map(_df.col(_))

    partitionColumns.size match {
      case 0 => _df
      case 1 => _df.repartition(partitionColumns: _*)
      case _ => _df.repartition(DEFAULT_FILES_PER_PARTITION, partitionColumns: _*)
    }
  }

  def addColumns(partitionColumnNames: Seq[String]): Repartitioner = {
    Repartitioner(this.partitionColumnNames ++ partitionColumnNames, this.builders)
  }

  def addBuilder(builder: PartitionBuilder): Repartitioner = {
    Repartitioner(this.partitionColumnNames, this.builders ++ Seq(builder))
  }

  def addColumnsWithBuilder(partitionColumnNames: Seq[String], builder: PartitionBuilder): Repartitioner = {
    this.addColumns(partitionColumnNames).addBuilder(builder)
  }
}