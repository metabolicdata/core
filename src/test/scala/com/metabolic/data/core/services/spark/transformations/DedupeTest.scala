package com.metabolic.data.core.services.spark.transformations

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.mapper.domain.ops.source.SQLOrder
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class DedupeTest extends AnyFunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfterAll{

  val inputData = Seq(
    Row("A", 2022, 2, 3),
    Row("B", 2022, 2, 4),
    Row("C", 2022, 2, 3),
    Row("C", 2022, 2, 3),
    Row("B", 2022, 2, 6),
    Row("C", 2022, 2, 6)
  )

  val someSchema = List(
    StructField("id", StringType, true),
    StructField("yyyy", IntegerType, true),
    StructField("mm", IntegerType, true),
    StructField("dd", IntegerType, true)
  )

  test("Dedupe truly duplicated rows") {
    val sqlCtx = sqlContext

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(someSchema)
    )

    val outputDF = inputDF
      .transform(new DedupeTransform(Seq("id"), Seq("yyyy","mm","dd")).dedupe())

    val expectedData = Seq(
      Row("A", 2022, 2, 3),
      Row("B", 2022, 2, 6),
      Row("C", 2022, 2, 6)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(someSchema)
    )

    assertDataFrameNoOrderEquals(outputDF, expectedDF)

  }

  test("Dedupe reverse order rows") {
    val sqlCtx = sqlContext

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(someSchema)
    )

    val outputDF = inputDF
      .transform(new DedupeTransform(Seq("id"), Seq("yyyy","mm","dd"), SQLOrder.withName("asc")).dedupe())

    val expectedData = Seq(
      Row("A", 2022, 2, 3),
      Row("B", 2022, 2, 4),
      Row("C", 2022, 2, 3)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(someSchema)
    )

    assertDataFrameNoOrderEquals(outputDF, expectedDF)

  }



}
