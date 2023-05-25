package com.metabolic.data.core.services.spark.partitioner

import org.apache.spark.sql.DataFrame;

trait PartitionBuilder {
        val partitionColumnNames: Seq[String]
        def addPartition(): DataFrame => DataFrame
}
