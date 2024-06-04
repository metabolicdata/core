package com.metabolic.data.core.services.spark.writer

import com.dimafeng.testcontainers.KafkaContainer
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.RegionedTest
import com.metabolic.data.core.services.spark.writer.file.DeltaWriter
import com.metabolic.data.mapper.domain.io.{EngineMode, WriteMode}
import io.delta.implicits.DeltaDataFrameReader
import io.delta.tables.DeltaTable
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.timeout
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Seconds, Span}

import java.io.File
import java.util.Properties
import scala.collection.JavaConverters._
import scala.reflect.io.Directory

class DeltaWriterStreamingTest extends AnyFunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfterAll
  with RegionedTest {

  override def conf: SparkConf = super.conf
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .set("spark.databricks.delta.optimize.repartition.enabled", "true")
    .set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")
    .set("spark.databricks.delta.retentionDurationCheck.enabled", "false")


  val lc_letters = Iterator("a", "b", "c", "d", "e", "f", "g", "h")
  val uc_letters = lc_letters.map { _.toUpperCase }

  val inputData = Seq(
    Row("A", "2022-02-05"),
    Row("b", "2022-02-04"),
    Row("C", "2022-02-03"),
    Row("d", "2022-02-02"),
    Row("E", "2022-02-01"),
    Row("f", "2022-01-05"),
    Row("G", "2021-02-02"),
    Row("h", "2020-02-05")
  )

  val someSchema = List(
    StructField("name", StringType, true),
    StructField("date", StringType, true),
  )

  test("Write Delta Append Streaming") {

    val sqlCtx = sqlContext

    val path = "src/test/tmp/delta/letters_streaming_kafka_append"
    val pathCheckpoint = s"$path/_checkpoint"

    val directoryPath = new Directory(new File(path))
    directoryPath.deleteRecursively()

    // Configuring kafka container
    val topicName = "letters_streaming_kafka_append"

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

    // Setting up a base Delta Table
    val batchDf = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(someSchema)
    )

    val firstWriter = new DeltaWriter(
      path,
      WriteMode.Overwrite,
      Option("date"),
      Option("name"),
      "default",
      "",
      Seq.empty[String],
      None)(region, spark)

    firstWriter.write(batchDf, EngineMode.Batch)

    // Setting up Kafka reader instead of Rate (to have more control over what gets sent)
    val inputDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaHost)
      .option("subscribe", topicName)
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(key AS STRING) as name", "CAST(value AS STRING) as date")

    //Produce to kafka
    val kafkaProducer = new KafkaProducer[String, String](properties)

    lc_letters.take(4).foreach { letter =>
      println(s"Sending letter $letter")
      val record = new ProducerRecord(topicName, s"$letter", "2022-02-06")
      kafkaProducer.send(record)
    }

    kafkaProducer.flush()

    // Appending Kafka contents to Delta Table
    val secondWriter = new DeltaWriter(
      path,
      WriteMode.Append,
      Option("date"),
      Option("name"),
      pathCheckpoint,
      "",
      Seq.empty[String], Option(true), Option(1), 168d)(region, spark)

    val query = secondWriter
      .write(inputDf, EngineMode.Stream)

    Thread.sleep(2000) // wait for 2 seconds

    lc_letters.drop(4).foreach { letter =>
      println(s"Sending letter $letter")
      val record = new ProducerRecord(topicName, s"$letter", "2022-02-07")
      kafkaProducer.send(record)
    }

    kafkaProducer.flush()
    kafkaProducer.close()

    val expectedData = Seq(
      Row("A", "2022-02-05"),
      Row("b", "2022-02-04"),
      Row("C", "2022-02-03"),
      Row("d", "2022-02-02"),
      Row("E", "2022-02-01"),
      Row("f", "2022-01-05"),
      Row("G", "2021-02-02"),
      Row("h", "2020-02-05"),
      Row("a", "2022-02-06"),
      Row("b", "2022-02-06"),
      Row("c", "2022-02-06"),
      Row("d", "2022-02-06"),
      Row("e", "2022-02-07"),
      Row("f", "2022-02-07"),
      Row("g", "2022-02-07"),
      Row("h", "2022-02-07")
    )

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(someSchema)
    )

    query.head.awaitTermination(30000)
    query.head.stop()

    val outputDf = spark.read.delta(path)
    assertDataFrameNoOrderEquals(outputDf, expectedDf)

  }

  test("Write Delta Upsert Streaming") {

    val sqlCtx = sqlContext

    val path = "src/test/tmp/delta/letters_streaming_kafka_upsert_name"
    val pathCheckpoint = s"$path/_checkpoint"

    val directoryPath = new Directory(new File(pathCheckpoint))
    directoryPath.deleteRecursively()

    // Configuring kafka container
    val topicName = "letters_streaming_kafka_upsert"

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

    // Setting up a base Delta Table
    val batchDf = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(someSchema)
    )

    val firstWriter = new DeltaWriter(
      path,
      WriteMode.Overwrite,
      None,
      None,
      "default",
      "",
      Seq.empty[String],
    )   (region, spark)

    firstWriter.write(batchDf, EngineMode.Batch)

    // Setting up Kafka reader instead of Rate (to have more control over what gets sent)
    val inputDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaHost)
      .option("subscribe", topicName)
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(key AS STRING) as name", "CAST(value AS STRING) as date")

    //Produce to kafka
    val kafkaProducer = new KafkaProducer[String, String](properties)

    kafkaProducer.send(new ProducerRecord(topicName, "a", "2022-02-06"))
    kafkaProducer.send(new ProducerRecord(topicName, "b", "2022-02-06"))
    kafkaProducer.send(new ProducerRecord(topicName, "c", "2022-02-06"))
    kafkaProducer.send(new ProducerRecord(topicName, "d", "2022-02-06"))

    kafkaProducer.flush()

    // Appending Kafka contents to Delta Table
    val secondWriter = new DeltaWriter(
      path,
      WriteMode.Upsert,
      None,
      Option("name"),
      pathCheckpoint,
      "",
      Seq.empty[String], Option(true), Option(1), 168d)(region, spark)

    val query = secondWriter
      .write(inputDf, EngineMode.Stream)

    val expectedData = Seq(
      Row("A", "2022-02-05"),
      Row("b", "2022-02-06"), // updated
      Row("C", "2022-02-03"),
      Row("d", "2022-02-06"), // updated
      Row("E", "2022-02-01"),
      Row("f", "2022-01-05"),
      Row("G", "2021-02-02"),
      Row("h", "2020-02-05"),
      Row("a", "2022-02-06"), // appended
      Row("c", "2022-02-06") // appended
    )

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(someSchema)
    )

    query.head.awaitTermination(21000)
    query.head.stop()
    query.last.awaitTermination(6000)
    query.last.stop()

    val outputDf = spark.read.delta(path)
    assertDataFrameNoOrderEquals(outputDf, expectedDf)


  }

  test("Write Delta Upsert With Timetravel Streaming") {

    val sqlCtx = sqlContext

    val path = "src/test/tmp/delta/letters_streaming_kafka_upsert_date"
    val pathCheckpoint = s"$path/_checkpoint"

    val directoryPath = new Directory(new File(pathCheckpoint))
    directoryPath.deleteRecursively()

    // Configuring kafka container
    val topicName = "letters_streaming_kafka_upsert_date"

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

    // Setting up a base Delta Table
    val batchDf = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(someSchema)
    )

    val firstWriter = new DeltaWriter(
      path,
      WriteMode.Overwrite,
      None,
      None,
      "default",
      "",
      Seq.empty[String], Option(false), Option(1), 168d)(region, spark)

    firstWriter.write(batchDf, EngineMode.Batch)

    // Setting up Kafka reader instead of Rate (to have more control over what gets sent)
    val inputDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaHost)
      .option("subscribe", topicName)
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(key AS STRING) as name", "CAST(value AS STRING) as date")

    //Produce to kafka
    val kafkaProducer = new KafkaProducer[String, String](properties)

    kafkaProducer.send(new ProducerRecord(topicName, "a", "2022-02-06")) // append
    kafkaProducer.send(new ProducerRecord(topicName, "b", "2022-02-04")) // hit
    kafkaProducer.send(new ProducerRecord(topicName, "d", "2022-02-06")) // append (miss)
    kafkaProducer.send(new ProducerRecord(topicName, "f", "2022-01-05")) // hit

    kafkaProducer.flush()

    // Appending Kafka contents to Delta Table
    val secondWriter = new DeltaWriter(
      path,
      WriteMode.Upsert,
      Option("date"),
      Option("name"),
      pathCheckpoint,
      "",
      Seq.empty[String], Option(true), Option(1), 168d)(region, spark)

    val query = secondWriter
      .write(inputDf, EngineMode.Stream)


    val expectedData = Seq(
      Row("A", "2022-02-05"),
      Row("b", "2022-02-04"),
      Row("C", "2022-02-03"),
      Row("d", "2022-02-02"),
      Row("d", "2022-02-06"), // append
      Row("E", "2022-02-01"),
      Row("f", "2022-01-05"),
      Row("G", "2021-02-02"),
      Row("h", "2020-02-05"),
      Row("a", "2022-02-06") // appended
    )

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(someSchema)
    )

    query.head.awaitTermination(21000)
    query.head.stop()
    query.last.awaitTermination(6000)
    query.last.stop()

    val outputDf = spark.read.delta(path)
    assertDataFrameNoOrderEquals(outputDf, expectedDf)


  }

  ignore("Optimize adhoc") {

    val outputPath = "src/test/tmp/delta/letters_streaming_kafka_upsert_name"
    val deltaTable = DeltaTable.forPath(outputPath)
    deltaTable.optimize().executeCompaction()
    deltaTable.vacuum(0)
  }

  ignore("Check Optimized") {
    val outputPath = "src/test/tmp/delta/letters_streaming_kafka_upsert_name"
    val deltaTable = DeltaTable.forPath(outputPath)

    //Get last operation
    val lastChange = deltaTable.history(1)
    val operation = lastChange.head().getAs[String]("operation")

    assert(operation == "OPTIMIZE")

  }


}
