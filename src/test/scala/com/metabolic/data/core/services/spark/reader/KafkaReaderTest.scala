package com.metabolic.data.core.services.spark.reader

import com.dimafeng.testcontainers.KafkaContainer
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.core.services.spark.reader.stream.KafkaReader
import com.metabolic.data.mapper.domain.run.EngineMode
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.util.Properties
import scala.collection.JavaConverters._

class KafkaReaderTest extends AnyFunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfterAll {

  val topicName = "test"

  test("A kafka server should return all data in kafka server")  {

    val container = KafkaContainer()
     container.start()

    val kafkaHost = container.bootstrapServers.replace("PLAINTEXT://", "")

    val adminProperties = new Properties()
    adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost)
    val adminClient = AdminClient.create(adminProperties)


    val createTopicResult = adminClient.createTopics(List(new NewTopic(topicName, 1, (1).toShort)).asJava)
    createTopicResult.values().get(topicName).get()

    val properties = new Properties()
    properties.put("bootstrap.servers", kafkaHost)
    properties.put("key.serializer", classOf[StringSerializer])
    properties.put("value.serializer", classOf[StringSerializer])

    val kafkaProducer = new KafkaProducer[String, String](properties)

    (0 to 5).foreach { i =>
      val record = new ProducerRecord(topicName, "key", s"event num $i")
      kafkaProducer.send(record)
    }

    kafkaProducer.flush()
    kafkaProducer.close()

    val data = Seq(
      Row("key", "event num 0"),
      Row("key", "event num 1"),
      Row("key", "event num 2"),
      Row("key", "event num 3"),
      Row("key", "event num 4"),
      Row("key", "event num 5")
    )

    val schema = List(
      StructField("key", StringType, true),
      StructField("value", StringType, true)
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema)
    )

    val reader = KafkaReader(Seq(kafkaHost), "", "", topicName)
    .read(spark, EngineMode.Batch)
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    assertDataFrameEquals(df, reader)

  }

}
