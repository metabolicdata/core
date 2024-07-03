package com.metabolic.data.core.services.schema

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.avro.functions.{from_avro, to_avro}
import org.apache.spark.sql.functions.{col, expr, struct, udf}
import org.json.JSONObject
import scalaj.http.{Http, HttpResponse}

import java.nio.ByteBuffer
import java.util
import java.util.Base64
import scala.collection.JavaConverters._
class CCloudSchemaRegistryService(schemaRegistryUrl: String, srApiKey: String, srApiSecret: String) extends Logging {

  private val props: util.Map[String, String] = Map(
    "basic.auth.credentials.source" -> "USER_INFO",
    "schema.registry.basic.auth.user.info" -> s"$srApiKey:$srApiSecret"
  ).asJava

  // UDF function
  private val binaryToStringUDF = udf((x: Array[Byte]) => BigInt(x).toString())

  def deserialize(topic: String, df: DataFrame): DataFrame = {
    // Get latest schema
    val avroSchema = getLastSchemaVersion(topic + "-value")

    // Remove first 5 bytes from value
    val dfFixed = df.withColumn("fixedValue", expr("substring(value, 6)"))

    // Get schema id from value
    val dfFixedId = dfFixed.withColumn("valueSchemaId", binaryToStringUDF(expr("value")))

    // Deserialize data
    val decoded_output = dfFixedId.select(
      from_avro(col("fixedValue"), avroSchema.get)
        .alias("value")
    )
    decoded_output.select("value.*")
  }


  def serialize(topic: String, df: DataFrame): DataFrame = {

    val schemaAvro = new AvroSchema(SchemaConverters.toAvroType(df.schema, recordName = "Envelope", nameSpace = topic))
    val schemaId = register(topic + "-value", schemaAvro.toString)


    // Serialize data to Avro format
    val serializedDF = df.select(to_avro(struct(df.columns.map(col): _*), schemaAvro.toString).alias("value"))

    // Add magic byte & schema id to the serialized data
    val addHeaderUDF = udf { (value: Array[Byte]) =>
      val magicByte: Byte = 0x0 // Assuming no magic byte is used
      val idBytes: Array[Byte] = ByteBuffer.allocate(4).putInt(schemaId.get).array()
      ByteBuffer.allocate(1 + idBytes.length + value.length)
        .put(magicByte)
        .put(idBytes)
        .put(value)
        .array()
    }

    // Apply the UDF to add header to each row
    val finalDF = serializedDF.withColumn("value", addHeaderUDF(col("value")))

    finalDF

  }

  private def register(subject: String, schema: String): Option[Int] = {
    val body = schema
    val request = s"$schemaRegistryUrl/subjects/$subject/versions"
    logger.info(s"Register schema for subject $subject")
    val credentials = s"$srApiKey:$srApiSecret"
    val base64Credentials = Base64.getEncoder.encodeToString(credentials.getBytes("utf-8"))

    try {
      val httpResponse: HttpResponse[String] = Http(request)
        .header("content-type", "application/octet-stream")
        .header("Authorization", s"Basic $base64Credentials")
        .postData(body.getBytes)
        .asString

      if (httpResponse.code == 200) {
        val jsonResponse = new JSONObject(httpResponse.body)
        val id = jsonResponse.getInt("id")
        logger.info(s"Schema registered for subject $subject with id: $id")
        Some(id)
      } else {
        logger.info(s"Error registering subject $subject: ${httpResponse.code} ${httpResponse.body}")
        throw new RuntimeException(s"Error registering subject $subject: ${httpResponse.code} ${httpResponse.body}")
      }
    } catch {
      case e: Exception =>
        logger.info("Error in registering schema: " + e.getMessage)
        throw e
    }
  }

  private def getLastSchemaVersion(subject: String): Option[String] = {
    val request = s"$schemaRegistryUrl/subjects/$subject/versions/latest"
    logger.info(s"Getting schema for subject $subject")
    val credentials = s"$srApiKey:$srApiSecret"
    val base64Credentials = Base64.getEncoder.encodeToString(credentials.getBytes("utf-8"))

    try {
      val httpResponse: HttpResponse[String] = Http(request)
        .header("Authorization", s"Basic $base64Credentials")
        .asString

      if (httpResponse.code == 200) {
        val jsonResponse = new JSONObject(httpResponse.body)
        val schema = jsonResponse.getString("schema")
        Some(schema)
      } else {
        logger.info(s"Error getting subject $subject: ${httpResponse.code} ${httpResponse.body}")
        throw new RuntimeException(s"Error registering subject $subject: ${httpResponse.code} ${httpResponse.body}")
      }
    } catch {
      case e: Exception =>
        logger.info("Error in getting schema: " + e.getMessage)
        throw e
    }
  }
}

