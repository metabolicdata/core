package com.metabolic.data.core.services.spark.reader.table

import com.metabolic.data.core.services.spark.reader.DataframeUnifiedReader
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.Random

class TableReader(fqn : String, enableJDBC: Boolean, queryOutputLocation: String) extends DataframeUnifiedReader {

  override val input_identifier: String = fqn

  override def readBatch(spark: SparkSession): DataFrame = {
    if(enableJDBC){
      spark.read
        .format("jdbc")
        .option("driver", "com.simba.athena.jdbc.Driver")
        .option("AwsCredentialsProviderClass", "com.simba.athena.amazonaws.auth.InstanceProfileCredentialsProvider")
        .option("url", "jdbc:awsathena://athena.eu-central-1.amazonaws.com:443")
        .option("dbtable", s"AwsDataCatalog.${input_identifier}")
        .option("S3OutputLocation", s"${queryOutputLocation}/${input_identifier}-${Random.nextInt(100000)}")
        .load()
    }else {
      spark.read
        .table(input_identifier)
    }
  }

  override def readStream(spark: SparkSession): DataFrame = {
    spark.readStream
      .table(input_identifier)
  }

}

object TableReader {
  def apply(fqn: String, enableJDBC: Boolean, queryOutputLocation: String) = new TableReader(fqn, enableJDBC, queryOutputLocation)

}