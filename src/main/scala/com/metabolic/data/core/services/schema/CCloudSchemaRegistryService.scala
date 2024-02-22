package com.metabolic.data.core.services.schema

import com.metabolic.data.core.services.catalogue.HttpRequestHandler
import com.metabolic.data.core.services.catalogue.HttpRequestHandler.logger
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, struct}
import za.co.absa.abris.avro.functions.{from_avro, to_avro}
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.abris.avro.registry.SchemaSubject
import za.co.absa.abris.config.{AbrisConfig, FromAvroConfig, ToAvroConfig}
import okhttp3._

import java.io.IOException
import org.apache.avro.Schema
import scalaj.http.{Http, HttpResponse}

class CCloudSchemaRegistryService(schemaRegistryUrl: String, srApiKey: String, srApiSecret: String) extends Logging {

  private val registryConfig = Map(
    AbrisConfig.SCHEMA_REGISTRY_URL -> schemaRegistryUrl,
    "basic.auth.credentials.source" -> "USER_INFO",
    "basic.auth.user.info" -> s"$srApiKey:$srApiSecret"
  )

  def deserializeWithAbris(topic: String, df: DataFrame): DataFrame = {
    val fromAvroConfigKey: FromAvroConfig = AbrisConfig
      .fromConfluentAvro
      .downloadReaderSchemaByLatestVersion
      .andTopicNameStrategy(topic, isKey = true)
      .usingSchemaRegistry(registryConfig)

    val fromAvroConfigValue: FromAvroConfig = AbrisConfig
      .fromConfluentAvro
      .downloadReaderSchemaByLatestVersion
      .andTopicNameStrategy(topic, isKey = false)
      .usingSchemaRegistry(registryConfig)


    df
      .select(from_avro(col("key"), fromAvroConfigKey).as("key"), from_avro(col("value"), fromAvroConfigValue).as("value"))
  }


  def serializeWithAbris(topic: String, df: DataFrame): DataFrame = {
    val schemaManager = SchemaManagerFactory.create(registryConfig)

    // register schema with topic name strategy
    val subject = SchemaSubject.usingTopicNameStrategy(topic, isKey = false) // Use isKey=true for the key schema and isKey=false for the value schema=
    schemaManager.register(subject, AvroSchemaUtils.toAvroSchema(df))


    val toAvroConfigValue: ToAvroConfig = AbrisConfig
      .toConfluentAvro
      .downloadSchemaByLatestVersion
      .andTopicNameStrategy(topic)
      .usingSchemaRegistry(registryConfig)

    val allColumns = struct(df.columns.head, df.columns.tail: _*)

    df.select(to_avro(allColumns, toAvroConfigValue) as 'value)


  }



  def register(subject: String, schema: Schema) {

    val body = schema.toString()
    val request = s"$schemaRegistryUrl/subjects/$subject/versions"

    logger.info(s"Register schema for subject $subject")

    try {
      val httpResponse: HttpResponse[String] = Http(request)
        .header("content-type", "application/octet-stream")
        .header("Authorization", s"Basic $srApiKey:$srApiSecret")
        .postData(body.getBytes)
        .asString

      val response = if (httpResponse.code == 200) httpResponse.body
      else {
        logger.info(s"Error registering subject $subject:  + ${httpResponse.code}  ${httpResponse.body}")
      }

      logger.info(s"Schema registered for subject $subject")




    } catch {
      case e: Exception => logger.info("Error in registering schema: " + e.getMessage)
    }
  }

}

