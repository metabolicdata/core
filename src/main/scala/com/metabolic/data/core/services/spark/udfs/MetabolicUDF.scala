package com.metabolic.data.core.services.spark.udfs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction

trait MetabolicUserDefinedFunction extends Serializable {
  def name: String
  def transformation: UserDefinedFunction
  def register(spark: SparkSession): Unit = {
    spark.udf.register(name, transformation)
  }

}
