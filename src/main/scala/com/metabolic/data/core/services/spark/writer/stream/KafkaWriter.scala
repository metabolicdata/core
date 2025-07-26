package com.metabolic.data.core.services.spark.writer.stream

import com.metabolic.data.core.services.spark.writer.DataframeUnifiedWriter
import com.metabolic.data.mapper.domain.io.WriteMode
import com.metabolic.data.mapper.domain.io.WriteMode.WriteMode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

class KafkaWriter(
                   servers: Seq[String],
                   apiKey: String,
                   apiSecret: String,
                   topic: String,
                   idColumnName: Option[String] = None,
                   val checkpointLocation: String
                 ) extends DataframeUnifiedWriter {

  override val output_identifier: String = topic
  override val writeMode: WriteMode = WriteMode.Append

  private val commonKafkaOptions: Map[String, String] = Map(
    "kafka.bootstrap.servers" -> servers.mkString(","),
    "topic" -> topic,
    "kafka.session.timeout.ms" -> "45000",
    "kafka.client.dns.lookup" -> "use_all_dns_ips"
  )

  private val authOptions: Map[String, String] =
    if (apiKey.nonEmpty && apiSecret.nonEmpty)
      Map(
        "kafka.security.protocol" -> "SASL_SSL",
        "kafka.sasl.mechanism" -> "PLAIN",
        "kafka.sasl.jaas.config" ->
          s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="$apiKey" password="$apiSecret";"""
      )
    else Map.empty

  private def prepareKafkaDf(df: DataFrame): DataFrame =
    idColumnName match {
      case Some(idCol) => df.selectExpr(s"$idCol as key", "to_json(struct(*)) as value")
      case None        => df.selectExpr("to_json(struct(*)) as value")
    }

  override def writeStream(df: DataFrame): StreamingQuery = {
    val kafkaDf = prepareKafkaDf(df)

    kafkaDf.writeStream
      .format("kafka")
      .options(commonKafkaOptions ++ authOptions)
      .option("checkpointLocation", checkpointLocation)
      .option("failOnDataLoss", value = false)
      .option("kafka.request.timeout.ms", "300000")         // 5 minutes
      .option("kafka.delivery.timeout.ms", "300000")        // Must be >= request.timeout.ms
      .option("kafka.retries", "10")
      .option("kafka.retry.backoff.ms", "1000")
      .start()
  }

  override def writeBatch(df: DataFrame): Unit = {
    val kafkaDf = prepareKafkaDf(df)

    kafkaDf.write
      .format("kafka")
      .options(commonKafkaOptions ++ authOptions)
      .save()
  }
}