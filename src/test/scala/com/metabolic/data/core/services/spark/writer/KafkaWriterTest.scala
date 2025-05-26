package com.metabolic.data.core.services.spark.writer

import com.dimafeng.testcontainers.KafkaContainer
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.core.services.spark.writer.stream.KafkaWriter
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.util.Properties
import scala.collection.JavaConverters._

class KafkaWriterTest extends AnyFunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfterAll {

  val topicName = "test-writer"

  override def beforeAll(): Unit = {
    super.beforeAll()
    System.setProperty("spark.sql.shuffle.partitions", "1")
  }

  test("KafkaWriter.writeBatch should write expected rows to Kafka and be verifiable") {

    val container = KafkaContainer()
    container.start()

    val kafkaHost = container.bootstrapServers.replace("PLAINTEXT://", "")

    // Create topic
    val adminProps = new Properties()
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost)
    val adminClient = AdminClient.create(adminProps)
    adminClient.createTopics(List(new NewTopic(topicName, 1, 1.toShort)).asJava)
      .values().get(topicName).get()

    val inputData = (0 to 5).map(i => Row("key", s"event num $i"))

    val schema = StructType(Seq(
      StructField("key", StringType, nullable = true),
      StructField("value", StringType, nullable = true)
    ))

    val inputDf = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      schema
    )

    val tempCheckpoint = new File(System.getProperty("java.io.tmpdir"), "kafka-writer-checkpoint").getAbsolutePath

    val writer = new KafkaWriter(
      servers = Seq(kafkaHost),
      apiKey = "",              // No auth
      apiSecret = "",
      topic = topicName,
      idColumnName = Some("key"),
      checkpointLocation = tempCheckpoint
    )

    writer.writeBatch(inputDf)

    // Wait briefly to ensure messages are written
    Thread.sleep(2000)

    val readDf = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaHost)
      .option("subscribe", topicName)
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    // Build expected JSON value DataFrame to match KafkaWriter output
    val expectedDf = inputDf
      .withColumn("value", to_json(struct(inputDf.columns.map(col): _*)))
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    val sortedExpected = expectedDf.orderBy("value")
    val sortedActual = readDf.orderBy("value")

    assertDataFrameEquals(sortedExpected, sortedActual)
  }
}
