package com.metabolic.data.core.services.spark.transformations

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class FlattenTest extends  AnyFunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfterAll{

  /*
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
  */

  val inputData = Seq(
    Row(Row("James ", "", "Smith"), Row(Row("CA", "Los Angles", 1), Row("CA", "Sandiago"))),
    Row(Row("Michael ", "Rose", ""), Row(Row("NY", "New York", 2), Row("NJ", "Newark"))),
    Row(Row("Robert ", "", "Williams"), Row(Row("DE", "Newark", 3), Row("CA", "Las Vegas"))),
    Row(Row("Maria ", "Anne", "Jones"), Row(Row("PA", "Harrisburg", 4), Row("CA", "Sandiago"))),
    Row(Row("Jen", "Mary", "Brown"), Row(Row("CA", "Los Angles", 5), Row("NJ", "Newark")))
  )

  val inputSchema = new StructType()
    .add("name", new StructType()
      .add("firstname", StringType)
      .add("middlename", StringType)
      .add("lastname", StringType))
    .add("address", new StructType()
      .add("current", new StructType()
        .add("state", StringType)
        .add("city", StringType)
        .add("postal_code", IntegerType))
      .add("previous", new StructType()
        .add("state", StringType)
        .add("city", StringType)))

  test("Flatten all structures") {
    val sqlCtx = sqlContext

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(inputSchema)
    )

    val outputDF = inputDF
      .transform(new FlattenTransform().flatten(Option.empty[String]))


    val expectedData = Seq(
      Row("James ", "", "Smith", "CA", "Los Angles", 1, "CA", "Sandiago"),
      Row("Michael ", "Rose", "", "NY", "New York", 2, "NJ", "Newark"),
      Row("Robert ", "", "Williams", "DE", "Newark", 3, "CA", "Las Vegas"),
      Row("Maria ", "Anne", "Jones", "PA", "Harrisburg", 4, "CA", "Sandiago"),
      Row("Jen", "Mary", "Brown", "CA", "Los Angles", 5, "NJ", "Newark")
    )

    val expectedSchema = List(
      StructField("name_firstname", StringType, true),
      StructField("name_middlename", StringType, true),
      StructField("name_lastname", StringType, true),
      StructField("address_current_state", StringType, true),
      StructField("address_current_city", StringType, true),
      StructField("address_current_postal_code", IntegerType, true),
      StructField("address_previous_state", StringType, true),
      StructField("address_previous_city", StringType, true)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    assertDataFrameNoOrderEquals(outputDF, expectedDF)

  }

  test("Flatten one column only") {
    val sqlCtx = sqlContext

    val columnOrderStatement = Seq("firstname", "middlename", "lastname", "address").map(col(_))

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(inputSchema)
    )

    val outputDF = inputDF
      .transform(new FlattenTransform().flatten(Option.apply("name")))
      .selectExpr("firstname","middlename","lastname","address")


    val expectedData = Seq(
      Row("James ", "", "Smith", Row(Row("CA", "Los Angles", 1), Row("CA", "Sandiago"))),
      Row("Michael ", "Rose", "",  Row(Row("NY", "New York", 2), Row("NJ", "Newark"))),
      Row("Robert ", "", "Williams", Row(Row("DE", "Newark", 3), Row("CA", "Las Vegas"))),
      Row("Maria ", "Anne", "Jones", Row(Row("PA", "Harrisburg", 4), Row("CA", "Sandiago"))),
      Row("Jen", "Mary", "Brown", Row(Row("CA", "Los Angles", 5), Row("NJ", "Newark")))
    )

    val expectedSchema = new StructType()
      .add("firstname", StringType)
      .add("middlename", StringType)
      .add("lastname", StringType)
      .add("address", new StructType()
        .add("current", new StructType()
          .add("state", StringType)
          .add("city", StringType)
          .add("postal_code", IntegerType)
        )
        .add("previous", new StructType()
          .add("state", StringType)
          .add("city", StringType)
        )
      )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    assertDataFrameNoOrderEquals(outputDF, expectedDF)

  }

}
