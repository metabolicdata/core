package com.metabolic.data.core.services.spark.writer.stream

import com.metabolic.data.core.services.schema.CCloudSchemaRegistryService
import com.metabolic.data.core.services.spark.writer.DataframeUnifiedWriter
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SaveMode}

class KafkaWriter(servers: Seq[String], apiKey: String, apiSecret: String, topic: String,
                  idColumnName: Option[String] = None, val checkpointLocation: String,
                  schemaRegistryUrl: String, srApiKey: String, srApiSecret: String, schemaRegistry: Option[String])
  extends DataframeUnifiedWriter {
  
  override val output_identifier: String = topic

  override val saveMode: SaveMode = SaveMode.Append

  override def writeStream(df: DataFrame): StreamingQuery = {

    val kafkaDf = schemaRegistry match {
      case Some("avro") =>
        new CCloudSchemaRegistryService(schemaRegistryUrl, srApiKey, srApiSecret).serializeWithAbris(output_identifier, df)
      case _ => {
        idColumnName match {
          case Some(c) => df.selectExpr(s"$c as key", "to_json(struct(*)) as value")
          case None => df.selectExpr("to_json(struct(*)) as value")
        }
      }
    }

    kafkaDf
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", servers.mkString(","))
      .option("topic", output_identifier)
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("kafka.sasl.jaas.config", s"org.apache.kafka.common.security.plain.PlainLoginModule required username='$apiKey' password='$apiSecret';")
      .option("kafka.session.timeout.ms", 45000)
      .option("kafka.client.dns.lookup","use_all_dns_ips")
      .option("checkpointLocation", checkpointLocation)
      .option("failOnDataLoss", false)
      .start()
    
  }

  override def writeBatch(df: DataFrame): Unit = {

    val kafkaDf = schemaRegistry match {
      case Some("avro") =>
        new CCloudSchemaRegistryService(schemaRegistryUrl, srApiKey, srApiSecret).serializeWithAbris(output_identifier, df)
      case _ => {
        idColumnName match {
          case Some(c) => df.selectExpr(s"$c as key", "to_json(struct(*)) as value")
          case None => df.selectExpr("to_json(struct(*)) as value")
        }
      }
    }

    kafkaDf
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", servers.mkString(","))
      .option("topic", output_identifier)
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("kafka.sasl.jaas.config", s"org.apache.kafka.common.security.plain.PlainLoginModule required username='$apiKey' password='$apiSecret';")
      .option("kafka.session.timeout.ms", 45000)
      .option("kafka.client.dns.lookup","use_all_dns_ips")
      .save()


  }

}
