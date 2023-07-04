package com.metabolic.data.core.services.spark.reader.stream

import com.metabolic.data.core.services.spark.reader.DataframeUnifiedReader
import org.apache.spark.sql.functions.{col, schema_of_json}
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

class KafkaReader(val servers: Seq[String], apiKey: String, apiSecret: String, topic: String, historical: Boolean)
  extends DataframeUnifiedReader {

  override val input_identifier: String = topic

  def setStreamAuthentication(r: DataStreamReader): DataStreamReader = {

    if(apiKey.isEmpty || apiSecret.isEmpty) {
      r
    } else {
      r
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", s"org.apache.kafka.common.security.plain.PlainLoginModule required username='$apiKey' password='$apiSecret';")
    }
  }

  def setDFAuthentication(r: DataFrameReader): DataFrameReader = {

    if (apiKey.isEmpty || apiSecret.isEmpty) {
      r
    } else {
      r
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", s"org.apache.kafka.common.security.plain.PlainLoginModule required username='$apiKey' password='$apiSecret';")
    }
  }

  def getSchema(spark: SparkSession): StructType = {

    val plain = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", servers.mkString(","))
      .option("subscribe", topic)
      .option("startingOffsets", "latest")

    val input = setDFAuthentication(plain)
      .load()

    val jsonString = input
      .selectExpr("CAST(value AS STRING) as value")
      .first()
      .toString()

    DataType.fromJson(jsonString).asInstanceOf[StructType]

  }

  def readStream(spark: SparkSession): DataFrame = {

    val plain = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", servers.mkString(","))
      .option("subscribe", topic)
      .option("kafka.session.timeout.ms", 45000)
      .option("kafka.client.dns.lookup","use_all_dns_ips")
      .option("startingOffsets", if (historical) "earliest" else "latest")
      .option("failOnDataLoss", false)


    val input = setStreamAuthentication(plain)
      .load()

    input.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

  }

  override def readBatch(spark: SparkSession): DataFrame = {

    val plain = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", servers.mkString(","))
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")

    val input = setDFAuthentication(plain)
      .load()

    input.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

  }

}

object KafkaReader {
  def apply(servers: Seq[String], apiKey: String, apiSecret: String, topic: String) = new KafkaReader(servers, apiKey, apiSecret, topic, false)
}
