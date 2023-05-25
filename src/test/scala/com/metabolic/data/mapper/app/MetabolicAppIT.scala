package com.metabolic.data.mapper.app

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.RegionedTest
import com.metabolic.data.core.domain.{Defaults, Environment}
import com.metabolic.data.core.services.util.ConfigReaderService
import com.metabolic.data.mapper.domain._
import com.metabolic.data.mapper.domain.io.{EngineMode, FileSink, FileSource, IOFormat}
import com.metabolic.data.mapper.domain.ops.sink.ManageSchemaSinkOp
import com.metabolic.data.mapper.domain.ops.{SQLFileMapping, SQLStatmentMapping}
import com.metabolic.data.mapper.services.ConfigParserService
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp
import java.util.Calendar

class MetabolicAppIT extends AnyFunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfterAll
  with RegionedTest {

  test("SQL In-line Config Integration") {
    val sqlCtx = sqlContext

    val fakeEmployeesData = Seq(
      Row("Marc", 33, 1),
      Row("Pau", 30, 1)
    )

    val someSchema = List(
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("version", IntegerType, true)
    )

    val fakeEmployeesDF = spark.createDataFrame(
      spark.sparkContext.parallelize(fakeEmployeesData),
      StructType(someSchema)
    )

    fakeEmployeesDF.write
      .mode("overwrite")
      .parquet("src/test/tmp/fake_employee")

    val multilineSQL = """|select *
                          |from employees
                          |where
                          | age
                          | <
                          | 40
                          |""".stripMargin

    val testingConfig = Config(
      "",
      List(FileSource("src/test/tmp/fake_employee", "employees", IOFormat.PARQUET)),
      List(SQLStatmentMapping(multilineSQL)),
      FileSink("test", "src/test/tmp/fake_employee_2", SaveMode.Overwrite, IOFormat.PARQUET)
    )

    MetabolicApp()
      .transformAll(List(testingConfig))(region,spark)

    val NT_fakeEmployeesDF = spark.read
      .parquet("src/test/tmp/fake_employee_2")

    assertDataFrameNoOrderEquals(fakeEmployeesDF, NT_fakeEmployeesDF)

  }

  test("SQL file Config Integration") {
    val sqlCtx = sqlContext

    val fakeEmployeesData = Seq(
      Row("Marc", 33, 1),
      Row("Pau", 30, 1)
    )

    val someSchema = List(
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("version", IntegerType, true)
    )

    val fakeEmployeesDF = spark.createDataFrame(
      spark.sparkContext.parallelize(fakeEmployeesData),
      StructType(someSchema)
    )

    fakeEmployeesDF.write
      .mode("overwrite")
      .parquet("src/test/tmp/f_fake_employee")


    val testingConfig = Config(
      "",
      List(io.FileSource("src/test/tmp/fake_employee", "employees", IOFormat.PARQUET)),
      List(new SQLFileMapping("src/test/resources/simple.sql", region)),
      io.FileSink("test", "src/test/tmp/f_fake_employee_t", SaveMode.Overwrite, IOFormat.PARQUET)

    )

    MetabolicApp()
      .transformAll(List(testingConfig))(region,spark)

    val NT_fakeEmployeesDF = spark.read
      .parquet("src/test/tmp/f_fake_employee_t")


    assertDataFrameNoOrderEquals(fakeEmployeesDF, NT_fakeEmployeesDF)

  }

  test("ConfigParserService Integration") {
    val sqlCtx = sqlContext

    val fakeEmployeesData = Seq(
      Row("Marc", 33, 1),
      Row("Pau", 30, 2)
    )

    val someSchema = List(
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("company", IntegerType, true)
    )

    val fakeEmployeesDF = spark.createDataFrame(
      spark.sparkContext.parallelize(fakeEmployeesData),
      StructType(someSchema)
    )

    fakeEmployeesDF.write
      .mode("overwrite")
      .parquet("src/test/tmp/fake_employee")

    val fakeCompaniesData = Seq(
      Row("MG.EU", 1),
      Row("Factorial", 2)
    )

    val someOtherSchema = List(
      StructField("name", StringType, true),
      StructField("id", IntegerType, true)
    )

    val fakeCompaniesDF = spark.createDataFrame(
      spark.sparkContext.parallelize(fakeCompaniesData),
      StructType(someOtherSchema)
    )

    fakeCompaniesDF.write
      .mode("overwrite")
      .parquet("src/test/tmp/fake_companies")



    val resultData = Seq(
      Row("Marc", 33, "MG.EU", 1),
      Row("Pau", 30, "Factorial", 1)
    )

    val resultSchema = List(
      StructField("employee_name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("company_name", StringType, true)
    )

    val resultDF = spark.createDataFrame(
      spark.sparkContext.parallelize(resultData),
      StructType(resultSchema)
    )


    val rawConfig = new ConfigReaderService()
      .getConfig("src/test/resources/employees.conf")

    val testingConfig = new ConfigParserService()
      .parseConfig(rawConfig)


    MetabolicApp()
      .transformAll(testingConfig)(region,spark)

    val NT_fakeEmployeesDF = spark.read
      .parquet("src/test/tmp/fake_employee_company_t")

    assertDataFrameNoOrderEquals(resultDF, NT_fakeEmployeesDF)
  }


  ignore("Append") {
    val sqlCtx = sqlContext

    //IN
    val fakeEmployeesData = Seq(
      Row("Marc", 1),
      Row("Pau", 2)
    )

    val someSchema = List(
      StructField("name", StringType, true),
      StructField("id", IntegerType, true)
    )

    val fakeEmployeesDF = spark.createDataFrame(
      spark.sparkContext.parallelize(fakeEmployeesData),
      StructType(someSchema)
    )

    //OUT
    val fakeOutputData = Seq(
      Row("Marc"),
      Row("Pau"),
      Row("Marc"),
      Row("Pau")
    )

    val outSchema = List(
      StructField("name", StringType, true))

    val fakeOutputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(fakeOutputData),
      StructType(outSchema)
    )

    fakeEmployeesDF.write
      .mode("overwrite")
      .parquet("src/test/tmp/fake_employee_append")

    fakeEmployeesDF.write
      .mode("overwrite")
      .parquet("src/test/tmp/fake_employee_append_nt")

    val testingConfig = Config(
      "",
      List(io.FileSource("src/test/tmp/fake_employee_append", "employees", IOFormat.PARQUET)),
      List(SQLStatmentMapping("select name from employees")),
      io.FileSink("test", "src/test/tmp/fake_employee_append_nt", SaveMode.Overwrite, IOFormat.PARQUET)
    )

    MetabolicApp()
      .transformAll(List(testingConfig))(region,spark)

    val NT_fakeEmployeesDF = spark.read
      .parquet("src/test/tmp/fake_employee_append_nt")

    assertDataFrameNoOrderEquals(fakeOutputDF, NT_fakeEmployeesDF)

  }
   ignore("Production testing") {

    val o = spark.read.parquet("src/test/resources/prod/old_companies")

    val testingConfig = Config(
      "",
      List(
        io.FileSource("src/test/resources/prod/old_companies", "factorial_s3_companies", IOFormat.PARQUET),
        io.FileSource("src/test/resources/prod/dl_singups", "factorial_clean_singups", IOFormat.PARQUET)),
      List(new SQLFileMapping("/Users/marcg/Documents/Code/Factorial/data-platform/mappings/funnel/companies.sql", region)),
      io.FileSink("test", "src/test/tmp/prod_companies_2", SaveMode.Overwrite, IOFormat.PARQUET, eventTimeColumnName = Option("completed_at"))
    )

    MetabolicApp()
      .transform(testingConfig)(spark, region)

    val t = spark.read.parquet("src/test/tmp/prod_companies_2")

    assert(t.count() == o.count())

    /*
    val rawConfig = ConfigReaderService.getConfig("src/test/resources/prod/signups.conf")

    val configs = ConfigParserService.parseConfig(rawConfig)

    EntityMapperApp.transformAll(configs)(region,spark)*/
  }

  ignore("Marketing Qualis testing") {

    val o = spark.read.parquet("src/test/resources/prod/marketing_qualifications")

    val now = Calendar.getInstance()
    now.set(2021,3,22,0,0,0)
    val ts = Timestamp.from(now.getTime.toInstant)

    val testingConfig = Config(
      "",
      List(
        io.FileSource("src/test/resources/prod/marketing_qualifications",
          "test_data_lake_raw_marketing_qualifications",
          IOFormat.PARQUET)
          /*SourceReadMode.DERIVED_SNAPSHOT,
          filterDate = ts,
          dedupeColumns = Seq("id"))*/
      ),
      List(new SQLFileMapping("/Users/marcg/Documents/Code/Factorial/data-platform/mappings/funnel/inbound_leads.sql",region)),
      io.FileSink("test", "src/test/tmp/prod_mkt_qualies_2", SaveMode.Overwrite, IOFormat.PARQUET, eventTimeColumnName = Option("updated_at"))

    )

    MetabolicApp()
      .transform(testingConfig)(spark, region)

    val t = spark.read.parquet("src/test/tmp/prod_mkt_qualies_2")

    assert(t.count() == o.count())

    /*
    val rawConfig = ConfigReaderService.getConfig("src/test/resources/prod/signups.conf")

    val configs = ConfigParserService.parseConfig(rawConfig)

    EntityMapperApp.transformAll(configs)(region,spark)*/
  }

  ignore("Production testing 2") {

    val o = spark.read.parquet("src/test/resources/prod/customers_mrr_no_rn/")

    val testingConfig = Config(
      "Customers with MRR and Animal",
      List(
        io.FileSource("src/test/resources/prod/customers_mrr_no_rn", "factorial_clean_customers_mrr", IOFormat.PARQUET)),//, readMode = SourceReadMode.DERIVED_SNAPSHOT)),
      List(new SQLFileMapping("mappings/funnel/customers_mrr_animal.sql",region)),
      io.FileSink("test", "src/test/tmp/prod_customers_mrr", SaveMode.Overwrite, IOFormat.PARQUET, eventTimeColumnName = Option("updated_at"))

    )

    MetabolicApp()
      .transform(testingConfig)(spark, region)

    val t = spark.read.parquet("src/test/tmp/prod_customers_mrr")

    assert(t.count() == o.count())

  }

  ignore("dateColumnTest") {
    val sqlCtx = sqlContext

    val rawConfig = new ConfigReaderService()
      .getConfig("src/test/resources/stripe_subs.conf")

    val testingConfig = new ConfigParserService()
      .parseConfig(rawConfig)


    MetabolicApp()
      .transformAll(testingConfig)(region,spark)


  }

  test("WITH statement in Select") {

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

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(someSchema)
    )

    inputDF.write.mode(SaveMode.Overwrite).parquet("src/test/tmp/vowelsv1_i")

    val testingConfig = Config(
      "Customers with MRR and Animal",
      List(
        io.FileSource("src/test/tmp/vowelsv1_i", "vowels", IOFormat.PARQUET)),
      List(SQLStatmentMapping("WITH a as (SELECT * from vowels) SELECT *, make_date(yyyy,mm,dd) as date from a")),
      io.FileSink("test", "src/test/tmp/vowelsv1_o", SaveMode.Overwrite, IOFormat.PARQUET, eventTimeColumnName = Option("date"), ops = Seq(ManageSchemaSinkOp())),
      Defaults(ConfigFactory.load()),
      Environment("", EngineMode.Batch, "", false,"dbName","",Option.empty,false, true)

    )

    MetabolicApp()
      .transform(testingConfig)(spark, region)

    val t = spark.read.parquet("src/test/tmp/vowelsv1_o")

    assert(t.count() == inputDF.count())

    t.printSchema()
    inputDF.printSchema()

    assert(t.columns.size == inputDF.columns.size + 2) //added "date"

  }

  test("Change Schema") {

    val inputDataV1 = Seq(
      Row("A", 2022, 2, 3, "1"),
      Row("B", 2022, 2, 4, "1"),
      Row("C", 2022, 2, 3, "1"),
      Row("A", 2022, 2, 3, "1"),
      Row("B", 2022, 2, 6, "1"),
      Row("C", 2022, 2, 6, "1")
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

    inputDFV1.write.mode("overwrite").parquet("src/test/tmp/vowels_i/version=1")

    val inputDataV2 = Seq(
      Row(1, 2022, 2, 3, "2"),
      Row(2, 2022, 2, 4, "2"),
      Row(3, 2022, 2, 3, "2"),
      Row(4, 2022, 2, 3, "2"),
      Row(5, 2022, 2, 6, "2"),
      Row(6, 2022, 2, 6, "2")
    )

    val someSchemaV2 = List(
      StructField("id_v2", IntegerType, true),
      StructField("yyyy", IntegerType, true),
      StructField("mm", IntegerType, true),
      StructField("dd", IntegerType, true),
      StructField("version", StringType, true)
    )

    val inputDFV2 = spark.createDataFrame(
      spark.sparkContext.parallelize(inputDataV2),
      StructType(someSchemaV2)
    )

    inputDFV2.write.mode("overwrite").parquet("src/test/tmp/vowels_i/version=2")

    // Add a new date column
    val testingConfig = Config(
      "Customers with MRR and Animal",
      List(
        io.FileSource("src/test/tmp/vowels_i", "vowels", IOFormat.PARQUET)),
      List(SQLStatmentMapping("WITH a as (SELECT * from vowels) SELECT *, CAST(make_date(yyyy,mm,dd) as string) as date from a")),
      io.FileSink("test", "src/test/tmp/vowels_o", SaveMode.Overwrite, IOFormat.PARQUET, eventTimeColumnName = Option("date")),
      Defaults(ConfigFactory.load()),
      Environment("", EngineMode.Batch, "",  false,"dbName","",Option.empty)

    )

    MetabolicApp()
      .transform(testingConfig)(spark, region)

    val t = spark.read.parquet("src/test/tmp/vowels_o")

    val expectedData = Seq(
      Row(null, 1, "2022-02-03", "1",2022, 2, 3),
      Row(null, 4, "2022-02-03", "1",2022, 2, 3),
      Row(null, 3, "2022-02-03", "1",2022, 2, 3),
      Row("C", null, "2022-02-03", "1",2022, 2, 3),
      Row("A", null, "2022-02-03", "1",2022, 2, 3),
      Row("A", null, "2022-02-03", "1",2022, 2, 3),
      Row(null, 2, "2022-02-04", "1",2022, 2, 4),
      Row("B", null, "2022-02-04", "1",2022, 2, 4),
      Row(null, 5, "2022-02-06", "1",2022, 2, 6),
      Row(null, 6, "2022-02-06", "1",2022, 2, 6),
      Row("C", null,"2022-02-06", "1",2022, 2, 6),
      Row("B", null, "2022-02-06", "1",2022, 2, 6)
    )

    val expectedSchema = List(
      StructField("id", StringType, true),
      StructField("id_v2", IntegerType, true),
      StructField("date", StringType, true),
      StructField("version", StringType, true),
      StructField("yyyy", IntegerType, true),
      StructField("mm", IntegerType, true),
      StructField("dd", IntegerType, true),
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    expectedDF.show()

    t.show()
    assert(t.count() == expectedDF.count())
    //assertDataFrameNoOrderEquals(t, expectedDF)

  }

}
