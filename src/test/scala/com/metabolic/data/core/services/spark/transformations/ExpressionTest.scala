package com.metabolic.data.core.services.spark.transformations

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class ExpressionTest extends AnyFunSuite
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

  test("With date column") {
    val sqlCtx = sqlContext

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(someSchema)
    )

    val outputDF = inputDF
      .transform(new ExpressionTransformation(Seq("make_date(yyyy,mm,dd) as date")).apply())

    val expectedData = Seq(
      Row("A", 2022, 2, 3, "2022-02-03"),
      Row("B", 2022, 2, 4, "2022-02-04"),
      Row("C", 2022, 2, 3, "2022-02-03"),
      Row("C", 2022, 2, 3, "2022-02-03"),
      Row("B", 2022, 2, 6, "2022-02-06"),
      Row("C", 2022, 2, 6, "2022-02-06")
    )

    val outputSchema = List(
      StructField("id", StringType, true),
      StructField("yyyy", IntegerType, true),
      StructField("mm", IntegerType, true),
      StructField("dd", IntegerType, true),
      StructField("date", StringType, true)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(outputSchema)
    ).withColumn("date", col("date").cast("date"))

    assertDataFrameNoOrderEquals(outputDF, expectedDF)

  }

  ignore("Multiple expresions") {
    val sqlCtx = sqlContext

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(someSchema)
    )

    val outputDF = inputDF
      .transform(new ExpressionTransformation(
        Seq(
          "make_date(yyyy,mm,dd) as date",
          "lower(id) as l_id"
        )
      ).apply())
      .selectExpr("id", "l_id", "date")

    val expectedData = Seq(
      Row("A", 2022, 2, 3, "2022-02-03", "a"),
      Row("B", 2022, 2, 4, "2022-02-04", "b"),
      Row("C", 2022, 2, 3, "2022-02-03", "c"),
      Row("C", 2022, 2, 3, "2022-02-03", "c"),
      Row("B", 2022, 2, 6, "2022-02-06", "b"),
      Row("c", 2022, 2, 6, "2022-02-06", "c")
    )

    val outputSchema = List(
      StructField("id", StringType, true),
      StructField("yyyy", IntegerType, true),
      StructField("mm", IntegerType, true),
      StructField("dd", IntegerType, true),
      StructField("date", StringType, true),
      StructField("l_id", StringType, true)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(outputSchema)
    ).withColumn("date", col("date").cast("date"))
      .selectExpr("id", "l_id", "date")

    assertDataFrameNoOrderEquals(outputDF, expectedDF)

  }


}
