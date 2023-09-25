package com.metabolic.data.core.services.spark.writer

import com.dimafeng.testcontainers.KafkaContainer
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.RegionedTest
import com.metabolic.data.core.services.spark.reader.file.DeltaReader
import com.metabolic.data.core.services.spark.reader.stream.KafkaReader
import com.metabolic.data.core.services.spark.writer.file.DeltaWriter
import com.metabolic.data.core.services.util.ConfigUtilsService
import com.metabolic.data.core.services.spark.writer.partitioned_file.DeltaPartitionWriter
import com.metabolic.data.mapper.domain.io.EngineMode
import io.delta.tables.DeltaTable
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.timeout
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Seconds, Span}

import java.util.Properties
import scala.collection.JavaConverters._
import scala.reflect.io.Directory
import java.io.File

class DeltaWriterTest extends AnyFunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfterAll
  with RegionedTest {

  override def conf: SparkConf = super.conf
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")


  val inputData = Seq(
    Row("A", "a", 2022, 2, 5, "2022-02-05"),
    Row("B", "b", 2022, 2, 4, "2022-02-04"),
    Row("C", "c", 2022, 2, 3, "2022-02-03"),
    Row("D", "d", 2022, 2, 2, "2022-02-02"),
    Row("E", "e", 2022, 2, 1, "2022-02-01"),
    Row("F", "f", 2022, 1, 5, "2022-01-05"),
    Row("G", "g", 2021, 2, 2, "2021-02-02"),
    Row("H", "h", 2020, 2, 5, "2020-02-05")
  )

  val someSchema = List(
    StructField("name", StringType, true),
    StructField("data", StringType, true),
    StructField("yyyy", IntegerType, true),
    StructField("mm", IntegerType, true),
    StructField("dd", IntegerType, true),
    StructField("date", StringType, true),
  )

  val path = "src/test/tmp/delta/letters_2"

  test("Tests Delta Overwrite New Data") {
    val sqlCtx = sqlContext


    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(someSchema)
    )

    //Create table
    val emptyRDD = spark.sparkContext.emptyRDD[Row]
    val emptyDF = spark.createDataFrame(emptyRDD, inputDF.schema)
    emptyDF
      .write
      .format("delta")
      .mode(SaveMode.Append)
      .save(path)

    val firstWriter = new DeltaPartitionWriter(
      Seq.empty[String],
      path,
      SaveMode.Overwrite,
      Option("date"),
      Option("name"),
      false,
      "default",
      "",
      Seq.empty[String]) (region, spark)

    firstWriter.write(inputDF, EngineMode.Batch)

    val updateData = Seq(
      Row("X", "z", 2022, 2, 5, "2022-02-06"),
      Row("Y", "y", 2022, 2, 4, "2022-02-05"),
      Row("Z", "z", 2022, 2, 3, "2022-02-05"),
      Row("A", "a_mod", 2022, 2, 5, "2022-02-06"),
      Row("B", "b_mod", 2022, 2, 4, "2022-02-05"),
      Row("C", "c", 2022, 2, 3, "2022-02-03"),
      Row("D", "d", 2022, 2, 2, "2022-02-02"),
      Row("E", "e", 2022, 2, 1, "2022-02-01")
    )

    val updateDF = spark.createDataFrame(
      spark.sparkContext.parallelize(updateData),
      StructType(someSchema)
    )

    val secondWriter = new DeltaPartitionWriter(Seq.empty[String], path, SaveMode.Overwrite,
      Option("date"), Option("name"), false, "default", "",Seq.empty[String]) (region, spark)

    secondWriter.write(updateDF, EngineMode.Batch)

    val expectedData = Seq(
      Row("X", "z", 2022, 2, 5, "2022-02-06"),
      Row("Y", "y", 2022, 2, 4, "2022-02-05"),
      Row("Z", "z", 2022, 2, 3, "2022-02-05"),
      Row("A", "a_mod", 2022, 2, 5, "2022-02-06"),
      Row("B", "b_mod", 2022, 2, 4, "2022-02-05"),
      Row("C", "c", 2022, 2, 3, "2022-02-03"),
      Row("D", "d", 2022, 2, 2, "2022-02-02"),
      Row("E", "e", 2022, 2, 1, "2022-02-01")
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(someSchema)
    )

    val outputDf = new DeltaReader(path)
      .read(sqlCtx.sparkSession, EngineMode.Batch)

    outputDf.show(20, false)

    assertDataFrameNoOrderEquals(expectedDF, outputDf)

  }

  test("Tests Delta Upsert New Data") {
    val sqlCtx = sqlContext

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(someSchema)
    )

    //Create table
    val emptyRDD = spark.sparkContext.emptyRDD[Row]
    val emptyDF = spark.createDataFrame(emptyRDD, inputDF.schema)
    emptyDF
      .write
      .format("delta")
      .mode(SaveMode.Append)
      .save(path)

    val firstWriter = new DeltaPartitionWriter(Seq.empty[String], path, SaveMode.Overwrite,
      Option("date"), Option("name"), false, "default", "", Seq.empty[String]) (region, spark)


    firstWriter.write(inputDF, EngineMode.Batch)

    val updateData = Seq(
      Row("X", "z", 2022, 2, 5, "2022-02-06"),
      Row("Y", "y", 2022, 2, 4, "2022-02-05"),
      Row("Z", "z", 2022, 2, 3, "2022-02-05"),
      Row("A", "a_mod", 2022, 2, 5, "2022-02-06"),
      Row("B", "b_mod", 2022, 2, 4, "2022-02-05"),
      Row("C", "c", 2022, 2, 3, "2022-02-03"),
      Row("D", "d", 2022, 2, 2, "2022-02-02"),
      Row("E", "e", 2022, 2, 1, "2022-02-01")
    )

    val updateDF = spark.createDataFrame(
      spark.sparkContext.parallelize(updateData),
      StructType(someSchema)
    )

    val secondWriter = new DeltaPartitionWriter(Seq.empty[String], path, SaveMode.Append,
      Option("date"), Option("name"), true, "default", "", Seq.empty[String]) (region, spark)

    secondWriter.write(updateDF, EngineMode.Batch)

    val expectedData = Seq(
      Row("A", "a_mod", 2022, 2, 5, "2022-02-06"),
      Row("A", "a", 2022, 2, 5, "2022-02-05"),
      Row("B", "b_mod", 2022, 2, 4, "2022-02-05"),
      Row("B", "b", 2022, 2, 4, "2022-02-04"),
      Row("C", "c", 2022, 2, 3, "2022-02-03"),
      Row("D", "d", 2022, 2, 2, "2022-02-02"),
      Row("E", "e", 2022, 2, 1, "2022-02-01"),
      Row("F", "f", 2022, 1, 5, "2022-01-05"),
      Row("G", "g", 2021, 2, 2, "2021-02-02"),
      Row("H", "h", 2020, 2, 5, "2020-02-05"),
      Row("X", "z", 2022, 2, 5, "2022-02-06"),
      Row("Y", "y", 2022, 2, 4, "2022-02-05"),
      Row("Z", "z", 2022, 2, 3, "2022-02-05")
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(someSchema)
    )

    val outputDf = new DeltaReader(path)
      .read(sqlCtx.sparkSession,EngineMode.Batch)

    outputDf.show(20, false)

    assertDataFrameNoOrderEquals(expectedDF, outputDf)

  }

  test("Tests Delta Upsert New Data With ID") {
    val sqlCtx = sqlContext

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(someSchema)
    )

    //Create table
    val emptyRDD = spark.sparkContext.emptyRDD[Row]
    val emptyDF = spark.createDataFrame(emptyRDD, inputDF.schema)
    emptyDF
      .write
      .format("delta")
      .mode(SaveMode.Append)
      .save(path)

    val firstWriter = new DeltaPartitionWriter(Seq.empty[String], path, SaveMode.Overwrite,
      Option(""),Option("name"), false, "default", "", Seq.empty[String])(region, spark)


    firstWriter.write(inputDF, EngineMode.Batch)

    val updateData = Seq(
      Row("X", "z", 2022, 2, 5, "2022-02-06"),
      Row("Y", "y", 2022, 2, 4, "2022-02-05"),
      Row("Z", "z", 2022, 2, 3, "2022-02-05"),
      Row("A", "a_mod", 2022, 2, 5, "2022-02-06"),
      Row("B", "b_mod", 2022, 2, 4, "2022-02-05"),
      Row("C", "c", 2022, 2, 3, "2022-02-03"),
      Row("D", "d", 2022, 2, 2, "2022-02-02"),
      Row("E", "e", 2022, 2, 1, "2022-02-01")
    )

    val updateDF = spark.createDataFrame(
      spark.sparkContext.parallelize(updateData),
      StructType(someSchema)
    )

    val secondWriter = new DeltaPartitionWriter(Seq.empty[String], path, SaveMode.Append,
      Option(""),Option("name"), true, "default", "", Seq.empty[String])(region, spark)

    secondWriter.write(updateDF, EngineMode.Batch)

    val expectedData = Seq(
      Row("A", "a_mod", 2022, 2, 5, "2022-02-06"),
      Row("B", "b_mod", 2022, 2, 4, "2022-02-05"),
      Row("C", "c", 2022, 2, 3, "2022-02-03"),
      Row("D", "d", 2022, 2, 2, "2022-02-02"),
      Row("E", "e", 2022, 2, 1, "2022-02-01"),
      Row("F", "f", 2022, 1, 5, "2022-01-05"),
      Row("G", "g", 2021, 2, 2, "2021-02-02"),
      Row("H", "h", 2020, 2, 5, "2020-02-05"),
      Row("X", "z", 2022, 2, 5, "2022-02-06"),
      Row("Y", "y", 2022, 2, 4, "2022-02-05"),
      Row("Z", "z", 2022, 2, 3, "2022-02-05")
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(someSchema)
    )

    val outputDf = new DeltaReader(path)
      .read(sqlCtx.sparkSession, EngineMode.Batch)

    outputDf.show(20, false)

    assertDataFrameNoOrderEquals(expectedDF, outputDf)

  }

  test("Tests Delta Append New Data") {
    val sqlCtx = sqlContext

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(someSchema)
    )

    //Create table
    val emptyRDD = spark.sparkContext.emptyRDD[Row]
    val emptyDF = spark.createDataFrame(emptyRDD, inputDF.schema)
    emptyDF
      .write
      .format("delta")
      .mode(SaveMode.Append)
      .save(path)

    val firstWriter = new DeltaPartitionWriter(Seq.empty[String], path, SaveMode.Overwrite,
      Option("date"), Option("name"), false, "default", "", Seq.empty[String]) (region, spark)


    firstWriter.write(inputDF, EngineMode.Batch)

    val updateData = Seq(
      Row("X", "z", 2022, 2, 5, "2022-02-06"),
      Row("Y", "y", 2022, 2, 4, "2022-02-05"),
      Row("Z", "z", 2022, 2, 3, "2022-02-05"),
      Row("A", "a_mod", 2022, 2, 5, "2022-02-06"),
      Row("B", "b_mod", 2022, 2, 4, "2022-02-05"),
      Row("C", "c", 2022, 2, 3, "2022-02-03"),
      Row("D", "d", 2022, 2, 2, "2022-02-02"),
      Row("E", "e", 2022, 2, 1, "2022-02-01")
    )

    val updateDF = spark.createDataFrame(
      spark.sparkContext.parallelize(updateData),
      StructType(someSchema)
    )

    val secondWriter = new DeltaPartitionWriter(Seq.empty[String], path, SaveMode.Append,
      Option("date"), Option("name"), false, "default", "", Seq.empty[String]) (region, spark)

    secondWriter.write(updateDF, EngineMode.Batch)

    val expectedData = Seq(
      Row("A", "a", 2022, 2, 5, "2022-02-05"),
      Row("B", "b", 2022, 2, 4, "2022-02-04"),
      Row("C", "c", 2022, 2, 3, "2022-02-03"),
      Row("D", "d", 2022, 2, 2, "2022-02-02"),
      Row("E", "e", 2022, 2, 1, "2022-02-01"),
      Row("F", "f", 2022, 1, 5, "2022-01-05"),
      Row("G", "g", 2021, 2, 2, "2021-02-02"),
      Row("H", "h", 2020, 2, 5, "2020-02-05"),
      Row("X", "z", 2022, 2, 5, "2022-02-06"),
      Row("Y", "y", 2022, 2, 4, "2022-02-05"),
      Row("Z", "z", 2022, 2, 3, "2022-02-05"),
      Row("A", "a_mod", 2022, 2, 5, "2022-02-06"),
      Row("B", "b_mod", 2022, 2, 4, "2022-02-05"),
      Row("C", "c", 2022, 2, 3, "2022-02-03"),
      Row("D", "d", 2022, 2, 2, "2022-02-02"),
      Row("E", "e", 2022, 2, 1, "2022-02-01")
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(someSchema)
    )

    val outputDf = new DeltaReader(path)
      .read(sqlCtx.sparkSession, EngineMode.Batch)

    outputDf.show(20, false)

    assertDataFrameNoOrderEquals(expectedDF, outputDf)

  }

  test("Tests Delta Append New Column") {

    val sqlCtx = sqlContext

    /*val inputData = Seq(
      Row("Alpha", "a", 2022, 2, 5, "2022-02-05", "extra"),
      Row("Beta", "b", 2022, 2, 4, "2022-02-04", "extra")
    )

    val someSchema = List(
      StructField("name", StringType, true),
      StructField("data", StringType, true),
      StructField("yyyy", IntegerType, true),
      StructField("mm", IntegerType, true),
      StructField("dd", IntegerType, true),
      StructField("date", StringType, true),
      StructField("extra", StringType, true)
    )

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(someSchema)
    )

    //val writer = new DeltaWriter(path, SaveMode.Append, Option("date"), Option("name"), "")

    //writer.write(inputDF, EngineMode.Batch)


    inputDF.write
      .format("delta")
      .mode("append")
      .option("mergeSchema", "true")
      .save(path)
*/

    val outputDf = new DeltaReader(path)
      .read(sqlCtx.sparkSession, EngineMode.Batch)

    println("SCHEMA: " + outputDf.schema.json)

    //outputDf.show(20, false)
  }

  test("Write Delta Streaming") {

    val sqlCtx = sqlContext

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(someSchema)
    )

    val firstWriter = new DeltaWriter(path, SaveMode.Overwrite,
      Option("date"), Option("name"), false, "", "", Seq.empty[String]) (region, spark)

    firstWriter.write(inputDF, EngineMode.Batch)

    eventually(timeout(Span(5, Seconds))) {
      val outputDf = new DeltaReader(path)
        .read(sqlCtx.sparkSession, EngineMode.Batch)
      assertDataFrameNoOrderEquals(inputDF, outputDf)
    }

  }

  test("Write Delta Streaming from kafka") {

    val sqlCtx = sqlContext

    val pathCheckpoint = "src/test/tmp/delta/checkpoint"
    val path = "src/test/tmp/delta/letters_3"

    val directoryPath = new Directory(new File(path))
    directoryPath.deleteRecursively()

    val directoryCheckpoint = new Directory(new File(pathCheckpoint))
    directoryCheckpoint.deleteRecursively()

    // Configuring kafka container
    val topicName = "test"

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


    //Set job reader + writer
    val reader = KafkaReader(Seq(kafkaHost), "", "", topicName, "", "", "", Option.empty)
      .read(spark, EngineMode.Stream)
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    /*
    val writer = new DeltaWriter(path, SaveMode.Append, Option("date"), Option("name"), false, pathCheckpoint)
    writer.write(reader, EngineMode.Stream)
    */

    //Create table
    val emptyRDD = spark.sparkContext.emptyRDD[Row]
    val emptyDF = spark.createDataFrame(emptyRDD, reader.schema)
    emptyDF
      .write
      .format("delta")
      .mode(SaveMode.Append)
      .save(path)

    val query = reader.writeStream
      .format("delta")
      .outputMode("append")
      .option("mergeSchema", "true")
      .option("checkpointLocation", pathCheckpoint)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start(path)
    query.awaitTermination(2000)

    //Produce to kafka
    val kafkaProducer = new KafkaProducer[String, String](properties)

    (0 to 5).foreach { i =>
      val record = new ProducerRecord(topicName, s"$i", s"event num $i")
      kafkaProducer.send(record)
    }

    kafkaProducer.flush()
    kafkaProducer.close()

    val data = Seq(
      Row("0", "event num 0"),
      Row("1", "event num 1"),
      Row("2", "event num 2"),
      Row("3", "event num 3"),
      Row("4", "event num 4"),
      Row("5", "event num 5")
    )

    val schema = List(
      StructField("key", StringType, true),
      StructField("value", StringType, true)
    )

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema)
    )

    eventually(timeout(Span(5, Seconds))) {
      val outputDf = new DeltaReader(path)
        .read(sqlCtx.sparkSession, EngineMode.Batch)
      assertDataFrameNoOrderEquals(inputDF, outputDf)
    }

  }

  test("Write Delta Streaming upsert from kafka") {

    val sqlCtx = sqlContext


    val pathCheckpoint = "src/test/tmp/delta/checkpoint_upsert"
    val path = "src/test/tmp/delta/letters_3"

    /*
    val directoryPath = new Directory(new File(path))
    directoryPath.deleteRecursively()

     */

    val directoryCheckpoint = new Directory(new File(pathCheckpoint))
    directoryCheckpoint.deleteRecursively()

    // Configuring kafka container
    val topicName = "test2"

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


    //Set job reader + writer
    val reader = KafkaReader(Seq(kafkaHost), "", "", topicName, "", "", "", Option.empty)
      .read(spark, EngineMode.Stream)
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    /*
    val writer = new DeltaWriter(path, SaveMode.Append, Option("date"), Option("name"), false, pathCheckpoint)
    writer.write(reader, EngineMode.Stream)
    */


    /*
    //Create table
    val emptyRDD = spark.sparkContext.emptyRDD[Row]
    val emptyDF = spark.createDataFrame(emptyRDD, reader.schema)
    emptyDF
      .write
      .format("delta")
      .mode(SaveMode.Append)
      .save(path)
     */


    val deltaTable = DeltaTable.forPath(path)

    def upsertToDelta(microBatchOutputDF: DataFrame, batchId: Long) {
      deltaTable.as("t")
        .merge(
          microBatchOutputDF.as("s"),
          "s.key = t.key")
        .whenMatched().updateAll()
        .whenNotMatched().insertAll()
        .execute()
    }

    val query = reader.writeStream
      .format("delta")
      .foreachBatch(upsertToDelta _)
      .outputMode("update")
      .option("mergeSchema", "true")
      .option("checkpointLocation", pathCheckpoint)
      .trigger(Trigger.ProcessingTime("15 seconds"))
      .start()

    query.awaitTermination(90000)

    //Produce to kafka
    val kafkaProducer = new KafkaProducer[String, String](properties)

    (0 to 6).foreach { i =>
      val record = new ProducerRecord(topicName, s"$i", s"event upsert num $i")
      kafkaProducer.send(record)
    }

    kafkaProducer.flush()
    kafkaProducer.close()

    //query.stop()


    val data = Seq(
      Row("0", "event upsert num 0"),
      Row("1", "event upsert num 1"),
      Row("2", "event upsert num 2"),
      Row("3", "event upsert num 3"),
      Row("4", "event upsert num 4"),
      Row("5", "event upsert num 5"),
      Row("6", "event upsert num 6")
    )

    val schema = List(
      StructField("key", StringType, true),
      StructField("value", StringType, true)
    )

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema)
    )

    eventually(timeout(Span(30, Seconds))) {
      val outputDf = new DeltaReader(path)
        .read(sqlCtx.sparkSession, EngineMode.Batch)
      assertDataFrameNoOrderEquals(inputDF, outputDf)
    }


  }

  test("Write Delta Streaming upsert new table from kafka") {

    val sqlCtx = sqlContext


    val pathCheckpoint = "src/test/tmp/delta/checkpoint_upsert"
    val path = "src/test/tmp/delta/letters_4"


    val directoryPath = new Directory(new File(path))
    directoryPath.deleteRecursively()
    val directoryCheckpoint = new Directory(new File(pathCheckpoint))
    directoryCheckpoint.deleteRecursively()

    // Configuring kafka container
    val topicName = "test3"

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


    //Set job reader + writer
    val reader = KafkaReader(Seq(kafkaHost), "", "", topicName, "", "", "", Option.empty)
      .read(spark, EngineMode.Stream)
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    /*
    val writer = new DeltaWriter(path, SaveMode.Append, Option("date"), Option("name"), false, pathCheckpoint)
    writer.write(reader, EngineMode.Stream)
    */


    //Create table
    val emptyRDD = spark.sparkContext.emptyRDD[Row]
    val emptyDF = spark.createDataFrame(emptyRDD, reader.schema)
    emptyDF
      .write
      .format("delta")
      .mode(SaveMode.Append)
      .save(path)

    val deltaTable = DeltaTable.forPath(path)

    def upsertToDelta(microBatchOutputDF: DataFrame, batchId: Long) {
      deltaTable.as("t")
        .merge(
          microBatchOutputDF.as("s"),
          "s.key = t.key")
        .whenMatched().updateAll()
        .whenNotMatched().insertAll()
        .execute()
    }

    val query = reader.writeStream
      .format("delta")
      .foreachBatch(upsertToDelta _)
      .outputMode("update")
      .option("mergeSchema", "true")
      .option("checkpointLocation", pathCheckpoint)
      .trigger(Trigger.ProcessingTime("15 seconds"))
      .start()

    query.awaitTermination(16000)

    //Produce to kafka
    val kafkaProducer = new KafkaProducer[String, String](properties)

    (0 to 6).foreach { i =>
      val record = new ProducerRecord(topicName, s"$i", s"event upsert num $i")
      kafkaProducer.send(record)
    }

    kafkaProducer.flush()
    kafkaProducer.close()



    val data = Seq(
      Row("0", "event upsert num 0"),
      Row("1", "event upsert num 1"),
      Row("2", "event upsert num 2"),
      Row("3", "event upsert num 3"),
      Row("4", "event upsert num 4"),
      Row("5", "event upsert num 5"),
      Row("6", "event upsert num 6")
    )

    val schema = List(
      StructField("key", StringType, true),
      StructField("value", StringType, true)
    )

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      StructType(schema)
    )

    eventually(timeout(Span(30, Seconds))) {
      val outputDf = new DeltaReader(path)
        .read(sqlCtx.sparkSession, EngineMode.Batch)
      assertDataFrameNoOrderEquals(inputDF, outputDf)
    }

  }

}


