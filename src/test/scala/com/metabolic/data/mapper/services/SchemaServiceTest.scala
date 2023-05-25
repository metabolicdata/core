package com.metabolic.data.mapper.services

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.RegionedTest
import com.metabolic.data.core.services.schema.SchemaService
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite


class SchemaServiceTest extends AnyFunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfterAll
  with RegionedTest {

  val firstVersion = Seq(
    Row(1, "Marc", true),
    Row(2, "Pau", false),
    Row(3, "Enric", true)
  )

  val firstSchema = List(
    StructField("id", IntegerType, true),
    StructField("name", StringType, true),
    StructField("property", BooleanType, true)
  )

  test("New Schema - Change Data Type, Rename Column And Add Version") {
    val secondVersion = Seq(
      Row(1, "Marc", "Toyota"),
      Row(2, "Pau", "Ford Motor"),
      Row(3, "Enric", "Daimler")
    )

    val secondSchema = List(
      StructField("id", StringType, true),
      StructField("name", StringType, true),
      StructField("property", StringType, true)
    )

    val secondInputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(secondVersion),
      StructType(secondSchema)
    )

    val firstInputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(firstVersion),
      StructType(firstSchema)
    )

    val outputDF =  new SchemaService()(spark).compareSchema(StructType(firstSchema), 1, secondInputDF)

    val expectedData = Seq(
      Row("1", "Marc", "Toyota"),
      Row("2", "Pau", "Ford Motor"),
      Row("3", "Enric", "Daimler")
    )

    val expectedSchema = List(
      StructField("id_v2", StringType, true),
      StructField("name", StringType, true),
      StructField("property_v2", StringType, true),
      StructField("version", IntegerType, false)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    assert(outputDF.schema, expectedDF.schema)

  }

  test("New Schema - Change Data Type Twice, Rename Column And Add Version") {

    val secondSchema = List(
      StructField("id", IntegerType, true),
      StructField("id_v2", StringType, true),
      StructField("name", StringType, true),
      StructField("property", StringType, true),
      StructField("version", IntegerType, false)
    )

    val thirdVersion = Seq(
      Row(3, 1, "Marc", "Toyota"),
      Row(2, 2, "Pau", "Ford Motor"),
      Row(1, 3, "Enric", "Daimler")
    )

    val thirdSchema = List(
      StructField("id", DoubleType, true),
      StructField("id_v2", StringType, true),
      StructField("name", StringType, true),
      StructField("property", StringType, true)
    )

    val thirdInputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(thirdVersion),
      StructType(thirdSchema)
    )

    val outputDF = new SchemaService()(spark).compareSchema(StructType(secondSchema), 2, thirdInputDF)
    val expectedSchema = Set(
      StructField("id_v2",StringType,true),
      StructField("id_v3", DoubleType, true),
      StructField("name", StringType, true),
      StructField("property", StringType, true),
      StructField("version", IntegerType, false),
    )

    assert(outputDF.schema.toSet, expectedSchema)

  }

  test("Get differences of two disitnct schemas") {

    val secondSchema = List(
      StructField("id", IntegerType, true),
      StructField("id_v2", StringType, true),
      StructField("name", StringType, true),
      StructField("property", StringType, true),
      StructField("version", IntegerType, false)
    )

    val thirdVersion = Seq(
      Row(1, "Marc", "Toyota"),
      Row(2, "Pau", "Ford Motor"),
      Row(3, "Enric", "Daimler")
    )

    val thirdSchema = List(
      StructField("id", DoubleType, true),
      StructField("name", StringType, true),
      StructField("property", StringType, true)
    )

    val thirdInputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(thirdVersion),
      StructType(thirdSchema)
    )

    val outputDF = new SchemaService()(spark).compareSchema(StructType(secondSchema), 2, thirdInputDF)
    val expectedSchema = Set(
      StructField("id_v3", DoubleType, true),
      StructField("name", StringType, true),
      StructField("property", StringType, true),
      StructField("version", IntegerType, false),
    )

    assert(outputDF.schema.toSet, expectedSchema)

  }

  test("Get differences of two distinct schemas") {

    val left = Set(
      StructField("id", IntegerType, false),
      StructField("id_v2", StringType, true),
      StructField("name", StringType, true),
      StructField("address", StringType, true)
    )

    val right = Set(
      StructField("id", StringType, false),
      StructField("email", StringType, true),
      StructField("address", BooleanType, true)
    )

    val identicalRight = Set(
      StructField("id", StringType, true),
      StructField("email", StringType, true),
      StructField("address", BooleanType)
    )

    val leftDiffExpected = Set(
      //StructField("id", StringType, true), // Because we already have id_v2 of type String
      StructField("email", StringType, true),
      StructField("address", BooleanType, true)
    )

    val rightDiffExpected = Set(
      StructField("id", IntegerType, false),
      StructField("name", StringType, true),
      StructField("address", StringType, true)
    )

    val newResult = new SchemaService()(spark).diffSchemas(left, right)

    assert(newResult.toSet == leftDiffExpected)


    val newRightResult = new SchemaService()(spark).diffSchemas(right, left)

    assert(newRightResult.toSet == rightDiffExpected)

    // Should not return any diff when only nullable value has changed.
    val newIdenticalResult = new SchemaService()(spark).diffSchemas(right, identicalRight)
    assert(newIdenticalResult.toSet == Nil.toSet)

  }

  test("Return Historical Version When Type Changes") {

    val oldSchema = Seq(
      StructField("id", IntegerType, true),
      StructField("id_v2", StringType, true),
      StructField("name", StringType, true),
      StructField("address", StringType, true)
    )

    val newSchema = Seq(
      StructField("id", StringType, true),
      StructField("email", StringType, true),
      StructField("address", BooleanType, true)
    )

    val newData = Seq(
      Row("1", "Marc", true)
    )

    val newDf = spark.createDataFrame(
      spark.sparkContext.parallelize(newData),
      StructType(newSchema)
    )

    val ExpectedSchema = StructType(Array(
      StructField("id_v2", StringType, true), // Because we already have id_v2 of type String
      StructField("email", StringType, true),
      StructField("address", BooleanType, true))
    )

    val newResult = new SchemaService()(spark).addVersioningToSchemaColumns(StructType(oldSchema), newDf)
    newResult.show()
    assert(newResult.schema == ExpectedSchema)

  }

  test("Return Historical Version, Same Types") {

    val oldSchema = Seq(
      StructField("id", IntegerType, true),
      StructField("id_v2", StringType, true),
      StructField("name", StringType, true),
      StructField("address", StringType, true)
    )

    val newSchema = Seq(
      StructField("id", IntegerType, true),
      StructField("email", StringType, true),
      StructField("address", BooleanType, true)
    )

    val newData = Seq(
      Row(1, "Marc", true)
    )

    val newDf = spark.createDataFrame(
      spark.sparkContext.parallelize(newData),
      StructType(newSchema)
    )

    val ExpectedSchema = StructType(Array(
      StructField("id", IntegerType, true), // Because we already have id_v2 of type String
      StructField("email", StringType, true),
      StructField("address", BooleanType, true))
    )

    val newResult = new SchemaService()(spark).addVersioningToSchemaColumns(StructType(oldSchema), newDf)
    newResult.show()
    assert(newResult.schema == ExpectedSchema)

  }

  test("New Schema - Add New Column, Add Version") {
    val secondVersion = Seq(
      Row(1, "Marc", true, true),
      Row(2, "Pau", false, true),
      Row(3, "Enric", true, false)
    )

    val secondSchema = List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("property", BooleanType, true),
      StructField("paid", BooleanType, true)
    )

    val secondInputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(secondVersion),
      StructType(secondSchema)
    )

    val firstInputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(firstVersion),
      StructType(firstSchema)
    )

    val outputDF =  new SchemaService()(spark)compareSchema(StructType(firstSchema), 1, secondInputDF)

    val expectedData = Seq(
      Row(1, "Marc", true, true, 1),
      Row(2, "Pau", false, true, 1),
      Row(3, "Enric", true, false, 1)
    )

    val expectedSchema = List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("property", BooleanType, true),
      StructField("paid", BooleanType, true),
      StructField("version", IntegerType, false)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    assert(outputDF.schema, expectedDF.schema)

  }

  test("New Schema - Remove A Column, Add Version") {
    val secondVersion = Seq(
      Row(1, true),
      Row(2, true),
      Row(3, false)
    )

    val secondSchema = List(
      StructField("id", IntegerType, true),
      StructField("property", BooleanType, true),
    )

    val secondInputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(secondVersion),
      StructType(secondSchema)
    )

    val firstInputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(firstVersion),
      StructType(firstSchema)
    )

    val outputDF =  new SchemaService()(spark)compareSchema(StructType(firstSchema), 1, secondInputDF)

    val expectedData = Seq(
      Row(1, true),
      Row(2, false),
      Row(3, true)
    )

    val expectedSchema = List(
      StructField("id", IntegerType, true),
      StructField("property", BooleanType, true),
      StructField("version", IntegerType, false)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    assert(outputDF.schema, expectedDF.schema)
    outputDF.show()

  }

  test("New Schema - Rename A Column, Add Version") {
    val secondVersion = Seq(
      Row(1, "Marc", true),
      Row(2, "Pau", false),
      Row(3, "Enric", true)
    )

    val secondSchema = List(
      StructField("id", IntegerType, true),
      StructField("firstname", StringType, true),
      StructField("property", BooleanType, true)
    )

    val secondInputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(secondVersion),
      StructType(secondSchema)
    )

    val firstInputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(firstVersion),
      StructType(firstSchema)
    )

    val outputDF =  new SchemaService()(spark)compareSchema(StructType(firstSchema), 1, secondInputDF)

    val expectedData = Seq(
      Row(1, "Marc", true),
      Row(2, "Pau", false),
      Row(3, "Enric", true)
    )

    val expectedSchema = List(
      StructField("id", IntegerType, true),
      StructField("firstname", StringType, true),
      StructField("property", BooleanType, true),
      StructField("version", IntegerType, false)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    assert(outputDF.schema, expectedDF.schema)

  }

  test("New Schema - Add New Column And Change Type, Add Version And Rename Column") {
    val secondVersion = Seq(
      Row(1, "Marc", "Toyota", true),
      Row(2, "Pau", "Ford Motor", true),
      Row(3, "Enric", "Daimler", false)
    )

    val secondSchema = List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("property", StringType, true),
      StructField("paid", BooleanType, true)
    )

    val secondInputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(secondVersion),
      StructType(secondSchema)
    )

    val firstInputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(firstVersion),
      StructType(firstSchema)
    )

    val outputDF =  new SchemaService()(spark)compareSchema(StructType(firstSchema), 1, secondInputDF)

    val expectedData = Seq(
      Row(1, "Marc", "Toyota", true),
      Row(2, "Pau", "Ford Motor", true),
      Row(3, "Enric", "Daimler", false)
    )

    val expectedSchema = List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("property_v2", StringType, true),
      StructField("paid", BooleanType, true),
      StructField("version", IntegerType, false)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    assert(outputDF.schema, expectedDF.schema)

  }

  test("First Time Table") {

    val schema = new SchemaService()(spark)

    val outputSchema = schema.getTableSchema("yolo","rolo")

    assert(outputSchema, new StructType(Array.empty[StructField]))

    val outputVersion = schema.getVersion("yolo","rolo")

    assert(outputVersion == schema.initialVersionValue)

  }

  test("Get Max Version For Existing Table") {

    val inputDataV1 = Seq(
      Row("A", 2022, 2, 3, "1"),
      Row("B", 2022, 2, 4, "1"),
      Row("C", 2022, 2, 3, "1"),
      Row("A", 2022, 2, 3, "2"),
      Row("B", 2022, 2, 6, "2"),
      Row("C", 2022, 2, 6, "2")
    )

    val someSchemaV1 = List(
      StructField("id", StringType, true),
      StructField("yyyy", IntegerType, true),
      StructField("mm", IntegerType, true),
      StructField("dd", IntegerType, true),
      StructField("version", StringType, true)
    )

    val inputDFV1 = spark.createDataFrame(
      spark.sparkContext.parallelize(inputDataV1),
      StructType(someSchemaV1)
    )

    val schema = new SchemaService()(spark)

    val outputVersion = schema.getMaxVersion(inputDFV1)

    assert(outputVersion == 2)

  }

  test("Test Contains String Version") {

    val inputDataV1 = Seq(
      Row("A", 2022, 2, 3, "1"),
      Row("B", 2022, 2, 4, "1"),
      Row("C", 2022, 2, 3, "1"),
      Row("A", 2022, 2, 3, "2"),
      Row("B", 2022, 2, 6, "2"),
      Row("C", 2022, 2, 6, "2")
    )

    val someSchemaV1 = List(
      StructField("id", StringType, true),
      StructField("yyyy", IntegerType, true),
      StructField("mm", IntegerType, true),
      StructField("dd", IntegerType, true),
      StructField("version", StringType, true)
    )

    val inputDFV1 = spark.createDataFrame(
      spark.sparkContext.parallelize(inputDataV1),
      StructType(someSchemaV1)
    )

    val schema = new SchemaService()(spark)

    val outputVersion = schema.containsVersion(inputDFV1.schema)

    assert(outputVersion, true)
  }

  test("Test Contains Integer Version") {

    val inputDataV1 = Seq(
      Row("A", 2022, 2, 3, 1),
      Row("B", 2022, 2, 4, 1),
      Row("C", 2022, 2, 3, 1),
      Row("A", 2022, 2, 3, 2),
      Row("B", 2022, 2, 6, 2),
      Row("C", 2022, 2, 6, 2)
    )

    val someSchemaV1 = List(
      StructField("id", StringType, true),
      StructField("yyyy", IntegerType, true),
      StructField("mm", IntegerType, true),
      StructField("dd", IntegerType, true),
      StructField("version", IntegerType, true)
    )

    val inputDFV1 = spark.createDataFrame(
      spark.sparkContext.parallelize(inputDataV1),
      StructType(someSchemaV1)
    )

    val schema = new SchemaService()(spark)

    val outputVersion = schema.containsVersion(inputDFV1.schema)

    assert(outputVersion, true)
  }

  test("Test CompareSchema") {
    val newData = Seq(
      Row(1, "m@y.com", "Marc", "2022-01-01", true)
    )

    val newSchema = List(
      StructField("id", IntegerType, true),
      StructField("email", StringType, true),
      StructField("first_name", StringType, true),
      StructField("updated_at", StringType, true),
      StructField("paid", BooleanType, true)
    )

    val df1 = spark.createDataFrame(
      spark.sparkContext.parallelize(newData),
      StructType(newSchema)
    )

    val oldData = Seq(
      Row(1, "m@y.com", "Marc", "G", "2022-01-01", 1)
    )

    val oldSchema = List(
      StructField("id", IntegerType, true),
      StructField("email", StringType, true),
      StructField("first_name", StringType, true),
      StructField("last_name", StringType, true),
      StructField("updated_at", StringType, true),
      StructField("version", IntegerType, true)
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(oldData),
      StructType(oldSchema)
    )

    val expectedSchema = Array(
      StructField("id", IntegerType, true),
      StructField("email", StringType, true),
      StructField("first_name", StringType, true),
      StructField("updated_at", StringType, true),
      StructField("paid", BooleanType, true),
      StructField("version",IntegerType,false)
    )
    val outputDF = new SchemaService()(spark) compareSchema(df.schema, 1, df1)
    assert(outputDF.schema == StructType(expectedSchema))
  }

  test("Special Character Removal - Get Table") {

    val test_data = Seq(
      Row(Row("jagdeesh", "", "rao"), List("Cricket", "Movies")),
      Row(Row("miRAJ", "kumar", ""), List("Tennis", "Reading")),
      Row(Row("sunDAR", "", "kumar"), List("Cooking", "Football")),
      Row(Row("ravi", "rampal", "kumar"), null)
    )

    val test_schema = new StructType()
      .add("full NAME", new StructType()
        .add("first-name", StringType)
        .add("middleNAME", StringType)
        .add("lastname%", StringType))
      .add("hobbies", ArrayType(StringType))
    val dfrdd = spark.sparkContext.parallelize(test_data)
    val inputDF = spark.createDataFrame(dfrdd, test_schema)

    val dbName = "default"
    val tableName = "clickup_test_schema"
    val schema = new SchemaService()(spark)

    val prep: DataFrame = schema.makeColumnsMetastoreCompatible(inputDF)

    prep.write.saveAsTable(s"$dbName.$tableName")

    val catalog = spark.catalog.tableExists("clickup_test_schema")

    val expected = if (catalog) {
      spark.table("clickup_test_schema")
    } else {
      spark.emptyDataFrame
    }

    val out = schema.getTable(dbName, tableName)(spark)

    assertDataFrameNoOrderEquals(expected, out)

  }

  test("Clean Schema Test") {

    val data = Seq(
      Row(Row("jagdeesh", "", "rao"), List("Cricket", "Movies"), Map("favourite_colour" -> "black", "country" -> "india")),
      Row(Row("miraj", "kumar", ""), List("Tennis", "Reading"), Map("favourite_colour" -> "brown", "country" -> "usa")),
      Row(Row("sundar", "", "kumar"), List("Cooking", "Football"), Map("favourite_colour" -> "red", "country" -> "india")),
      Row(Row("raVI", "rampal", "kumar"), null, Map("favourite_colour" -> "blond", "country" -> "malaysia")),
      Row(Row("JANU", "mani", "kumari"), List("football", "longdrive"), Map("favourite_colour" -> "black", "country" -> "india")),
      Row(Row("ravan", "", "lankha"), List("Blogging", "reading"), Map("favourite_colour" -> "black", "country" -> "uk"))
    )

    val schema = new StructType()
      .add("full_name", new StructType()
        .add("first-name", StringType)
        .add("middlename", StringType)
        .add("lastname%", StringType))
      .add("hobbies", ArrayType(StringType))
      .add("details", MapType(StringType, StringType))
    val dfrdd = spark.sparkContext.parallelize(data)
    val final_df = spark.createDataFrame(dfrdd, schema)

    val schemaSer = new SchemaService()(spark)

    val prep: DataFrame = schemaSer.makeColumnsMetastoreCompatible(final_df)

    val expectedSchema = new StructType()
      .add("full_name", new StructType()
        .add("first_name", StringType)
        .add("middlename", StringType)
        .add("lastname_", StringType))
      .add("hobbies", ArrayType(StringType))
      .add("details", MapType(StringType, StringType))

    assert(prep.schema, expectedSchema)

  }

  test("Test complex diff schema") {

    val leftDt = new StructType()
      .add("full_name", new StructType()
        .add(StructField("middlename", StringType, false))
        .add(StructField("first_name", StringType, true))
        .add(StructField("lastname", ArrayType(StringType), false)))
      .add("hobbies", ArrayType(StringType))
      .add("details", MapType(StringType, StringType))

    val s = new SchemaService()(spark)

    val rightDt = new StructType()
      .add("full_name", new StructType()
        .add("first_name", StringType)
        .add("middlename", StringType)
        .add("lastname", ArrayType(StringType)))
      .add("hobbies", ArrayType(StringType))
      .add("details", MapType(StringType, StringType))

    val left = Set(StructField("history_items_v1", s.safeStruct(leftDt), false), StructField("history_items_v2", s.safeStruct(rightDt), true))
    val right = Set(StructField("history_items_v1", s.safeStruct(leftDt), true), StructField("history_items_v2", s.safeStruct(rightDt), true))
    val newResult = s.diffSchemas(left, right)
    assert(newResult.toSet == Nil.toSet)

  }
}
