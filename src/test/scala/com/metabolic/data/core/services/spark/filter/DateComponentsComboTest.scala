package com.metabolic.data.core.services.spark.filter

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.joda.time.DateTime
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class DateComponentsComboTest extends AnyFunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfterAll {

  val inputData = Seq(
    Row("A", 2022, 2, 5),
    Row("B", 2022, 2, 4),
    Row("C", 2022, 2, 3),
    Row("D", 2022, 2, 2),
    Row("E", 2022, 2, 1),
    Row("F", 2022, 1, 5),
    Row("G", 2021, 2, 2),
    Row("H", 2020, 2, 5),
  )

  val someSchema = List(
    StructField("name", StringType, true),
    StructField("yyyy", IntegerType, true),
    StructField("mm", IntegerType, true),
    StructField("dd", IntegerType, true)
  )

  test("Filters rows with between 4/2/22 and mid 5/2/22") {
    val sqlCtx = sqlContext

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(someSchema)
    )

    val outputDF = inputDF
      //Start of 4/2/22
      .transform(new DateComponentsFromReader(new DateTime(1643932800000L), depth = DataLakeDepth.DAY).filter())
      //Mid day of 5/2/22
      .transform(new DateComponentsUpToReader(new DateTime(1644067847000L), depth = DataLakeDepth.DAY).filter())

    val expectedData = Seq(
      Row("A", 2022, 2, 5),
      Row("B", 2022, 2, 4)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(someSchema)
    )

    assertDataFrameNoOrderEquals(outputDF, expectedDF)

  }

  test("Filters rows with between 4/2/22 and mid 5/2/22 as Strings") {
    val sqlCtx = sqlContext

    val stringInputData = Seq(
      Row("X", "2022", "02", "06"),
      Row("Y", "2022", "03", "05"),
      Row("Z", "2023", "02", "05"),
      Row("A", "2022", "02", "05"),
      Row("B", "2022", "02", "04"),
      Row("C", "2022", "02", "03"),
      Row("D", "2022", "02", "02"),
      Row("E", "2022", "02", "01"),
      Row("F", "2022", "01", "05"),
      Row("G", "2021", "02", "02"),
      Row("H", "2020", "02", "05"),
    )

    val stringSchema = List(
      StructField("name", StringType, true),
      StructField("yyyy", StringType, true),
      StructField("mm", StringType, true),
      StructField("dd", StringType, true)
    )

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(stringInputData),
      StructType(stringSchema)
    )

    val outputDF = inputDF
      //Start of 4/2/22
      .transform(new DateComponentsFromReader(new DateTime(1643932800000L), depth = DataLakeDepth.DAY).filter())
      //Mid day of 5/2/22
      .transform(new DateComponentsUpToReader(new DateTime(1644067847000L), depth = DataLakeDepth.DAY).filter())

    val stringExpectedData = Seq(
      Row("A", "2022", "02", "05"),
      Row("B", "2022", "02", "04")
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(stringExpectedData),
      StructType(stringSchema)
    )

    assertDataFrameNoOrderEquals(outputDF, expectedDF)

  }

}
