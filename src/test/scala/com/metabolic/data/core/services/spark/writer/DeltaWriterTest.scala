package com.metabolic.data.core.services.spark.writer

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.RegionedTest
import com.metabolic.data.core.services.spark.reader.file.DeltaReader
import com.metabolic.data.core.services.spark.writer.file.DeltaWriter
import com.metabolic.data.mapper.domain.io.{EngineMode, WriteMode}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class DeltaWriterTest extends AnyFunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfterAll
  with RegionedTest {

  override def conf: SparkConf = super.conf
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .set("spark.databricks.delta.optimize.repartition.enabled","true")
    .set("spark.databricks.delta.vacuum.parallelDelete.enabled","true")
    .set("spark.databricks.delta.retentionDurationCheck.enabled","false")


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

  test("Tests Delta Overwrite New Data") {
    val path = "src/test/tmp/delta/letters_overwrite"

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

    val firstWriter = new DeltaWriter(
      path,
      WriteMode.Overwrite,
      Option("date"),
      Option("name"),
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

    val secondWriter = new DeltaWriter( path,
      WriteMode.Overwrite,
      Option("date"), Option("name"), "default", "",Seq.empty[String],0d) (region, spark)

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

    //outputDf.show(20, false)

    assertDataFrameNoOrderEquals(expectedDF, outputDf)

  }

  test("Tests Delta Append New Data") {
    val path = "src/test/tmp/delta/letters_append"
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

    val firstWriter = new DeltaWriter(
      path,
      WriteMode.Overwrite,
      Option("date"),
      Option("name"),
      "default",
      "",
      Seq.empty[String])(region, spark)


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

    val secondWriter = new DeltaWriter(path,
      WriteMode.Append,
      Option("date"), Option("name"), "default", "", Seq.empty[String])(region, spark)

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

    //outputDf.show(20, false)

    assertDataFrameNoOrderEquals(expectedDF, outputDf)

  }

  test("Tests Delta Upsert New Data on ID (name)") {
    val path = "src/test/tmp/delta/letters_upsert_id"

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

    val firstWriter = new DeltaWriter(
      path,
      WriteMode.Overwrite,
      Option(""),
      Option("name"),
      "default",
      "",
      Seq.empty[String])(region, spark)


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

    val secondWriter = new DeltaWriter(
      path,
      WriteMode.Upsert,
      Option(""),
      Option("name"),
      "default",
      "",
      Seq.empty[String])(region, spark)

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

    //outputDf.show(20, false)

    assertDataFrameNoOrderEquals(expectedDF, outputDf)

  }

  test("Tests Delta Upsert New Data on ID (name) and date ") {
    val path = "src/test/tmp/delta/letters_upsert_id_date"

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

    val firstWriter = new DeltaWriter( path,
      WriteMode.Overwrite,
      Option("date"), Option("name"), "default", "", Seq.empty[String]) (region, spark)

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

    val secondWriter = new DeltaWriter(
      path,
      WriteMode.Upsert,
      Option("date"),
      Option("name"),
      "default",
      "",
      Seq.empty[String]) (region, spark)

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

    //outputDf.show(20, false)

    assertDataFrameNoOrderEquals(expectedDF, outputDf)

  }

  test("Tests Delta Append New Column") {

    val path = "src/test/tmp/delta/letters_append_new_column"

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

    val firstWriter = new DeltaWriter(path,
      WriteMode.Overwrite,
      Option("date"), Option("name"), "default", "", Seq.empty[String])(region, spark)

    firstWriter.write(inputDF, EngineMode.Batch)


    val updateData = Seq(
      Row("Alpha", "a", 2022, 2, 5, "2022-02-05", "extra"),
      Row("Beta", "b", 2022, 2, 4, "2022-02-04", "extra")
    )

    val updateSchema = List(
      StructField("name", StringType, true),
      StructField("data", StringType, true),
      StructField("yyyy", IntegerType, true),
      StructField("mm", IntegerType, true),
      StructField("dd", IntegerType, true),
      StructField("date", StringType, true),
      StructField("extra", StringType, true)
    )

    val updateDF = spark.createDataFrame(
      spark.sparkContext.parallelize(updateData),
      StructType(updateSchema)
    )

    val secondWriter = new DeltaWriter(path,
      WriteMode.Append,
      Option("date"),
      Option("name"),
      "default",
      "",
      Seq.empty[String])(region, spark)

    secondWriter.write(updateDF, EngineMode.Batch)


    val expectedData = Seq(
      Row("A", "a", 2022, 2, 5, "2022-02-05", null),
      Row("B", "b", 2022, 2, 4, "2022-02-04", null),
      Row("C", "c", 2022, 2, 3, "2022-02-03", null),
      Row("D", "d", 2022, 2, 2, "2022-02-02", null),
      Row("E", "e", 2022, 2, 1, "2022-02-01", null),
      Row("F", "f", 2022, 1, 5, "2022-01-05", null),
      Row("G", "g", 2021, 2, 2, "2021-02-02", null),
      Row("H", "h", 2020, 2, 5, "2020-02-05", null),
      Row("Alpha", "a", 2022, 2, 5, "2022-02-05", "extra"),
      Row("Beta", "b", 2022, 2, 4, "2022-02-04", "extra")
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(updateSchema)
    )

    val outputDf = new DeltaReader(path)
      .read(sqlCtx.sparkSession, EngineMode.Batch)

    //outputDf.show(20, false)

    assertDataFrameNoOrderEquals(expectedDF, outputDf)
  }

  test("Tests Delta Upsert New Column") {

    val path = "src/test/tmp/delta/letters_upsert_new_column"

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

    val firstWriter = new DeltaWriter(path,
      WriteMode.Overwrite,
      Option("date"), Option("name"),
      "default", "", Seq.empty[String])(region, spark)

    firstWriter.write(inputDF, EngineMode.Batch)


    val updateData = Seq(
      Row("A", "alpha", 2022, 2, 5, "2022-02-05", "extra"),
      Row("B", "beta", 2022, 2, 4, "2022-02-04", "extra")
    )

    val updateSchema = List(
      StructField("name", StringType, true),
      StructField("data", StringType, true),
      StructField("yyyy", IntegerType, true),
      StructField("mm", IntegerType, true),
      StructField("dd", IntegerType, true),
      StructField("date", StringType, true),
      StructField("extra", StringType, true)
    )

    val updateDF = spark.createDataFrame(
      spark.sparkContext.parallelize(updateData),
      StructType(updateSchema)
    )

    val secondWriter = new DeltaWriter(path,
      WriteMode.Upsert,
      Option("date"),
      Option("name"),
      "default",
      "",
      Seq.empty[String])(region, spark)

    secondWriter.write(updateDF, EngineMode.Batch)


    val expectedData = Seq(
      Row("A", "alpha", 2022, 2, 5, "2022-02-05", "extra"),
      Row("B", "beta", 2022, 2, 4, "2022-02-04", "extra"),
      Row("C", "c", 2022, 2, 3, "2022-02-03", null),
      Row("D", "d", 2022, 2, 2, "2022-02-02", null),
      Row("E", "e", 2022, 2, 1, "2022-02-01", null),
      Row("F", "f", 2022, 1, 5, "2022-01-05", null),
      Row("G", "g", 2021, 2, 2, "2021-02-02", null),
      Row("H", "h", 2020, 2, 5, "2020-02-05", null)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(updateSchema)
    )

    val outputDf = new DeltaReader(path)
      .read(sqlCtx.sparkSession, EngineMode.Batch)

    //outputDf.show(20, false)

    assertDataFrameNoOrderEquals(expectedDF, outputDf)
  }

  test("Tests Delta Delete") {

    val path = "src/test/tmp/delta/letters_delete"

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

    val firstWriter = new DeltaWriter(path,
      WriteMode.Overwrite,
      Option("date"), Option("name"),
      "default", "", Seq.empty[String])(region, spark)

    firstWriter.write(inputDF, EngineMode.Batch)

    val deleteData = Seq(
      Row("A", "2022-02-05", "some"),
      Row("B", "2022-02-04", "other")
    )

    val deleteSchema = List(
      StructField("name", StringType, true),
      StructField("date", StringType, true),
      StructField("ignore", StringType, true)
    )

    val deleteDF = spark.createDataFrame(
      spark.sparkContext.parallelize(deleteData),
      StructType(deleteSchema)
    )

    val secondWriter = new DeltaWriter(path,
      WriteMode.Delete,
      Option("date"),
      Option("name"),
      "default",
      "",
      Seq.empty[String])(region, spark)

    secondWriter.write(deleteDF, EngineMode.Batch)


    val expectedData = Seq(
      Row("C", "c", 2022, 2, 3, "2022-02-03"),
      Row("D", "d", 2022, 2, 2, "2022-02-02"),
      Row("E", "e", 2022, 2, 1, "2022-02-01"),
      Row("F", "f", 2022, 1, 5, "2022-01-05"),
      Row("G", "g", 2021, 2, 2, "2021-02-02"),
      Row("H", "h", 2020, 2, 5, "2020-02-05"),
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

}


