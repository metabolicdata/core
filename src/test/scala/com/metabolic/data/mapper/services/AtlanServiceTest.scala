package com.metabolic.data.mapper.services

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.RegionedTest
import com.metabolic.data.core.domain.{Defaults, Environment}
import com.metabolic.data.core.services.catalogue.AtlanService
import com.metabolic.data.core.services.util.ConfigUtilsService
import com.metabolic.data.mapper.domain._
import com.metabolic.data.mapper.domain.io.{EngineMode, IOFormat}
import com.metabolic.data.mapper.domain.ops.SQLFileMapping
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SaveMode
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class AtlanServiceTest extends AnyFunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfterAll
  with RegionedTest {

  test("Test source table name with infix"){
    val s3Path = "foo/bar/table_name"
    val prefix_namespaces = Seq("foo", "hello", "some")
    val infix_namespaces = Seq("foo_bar", "hello_world", "some_other_namespace")
    val prefix = ConfigUtilsService.getTablePrefix(prefix_namespaces, s3Path)
    val infix = ConfigUtilsService.getTableInfix(infix_namespaces, s3Path)
    val tableName = ConfigUtilsService.getTableNameFileSink(s3Path)
    val expected = "foo_bar_table_name"
    assert(prefix+infix+tableName, expected)
  }

  test("Test source table name without infix") {
    val s3Path = "foo/table_name"
    val prefix_namespaces = Seq("foo", "hello", "some")
    val infix_namespaces = Seq("foo_bar", "hello_world", "some_other_namespace")
    val prefix = ConfigUtilsService.getTablePrefix(prefix_namespaces, s3Path)
    val infix = ConfigUtilsService.getTableInfix(infix_namespaces, s3Path)
    val tableName = ConfigUtilsService.getTableNameFileSink(s3Path)
    val expected = "foo_table_name"
    assert(prefix + infix + tableName, expected)
  }

  test("Test generated json for lineage") {

    val testingConfig = Config(
      "",
      List(io.FileSource("raw/stripe/fake_employee/version=3/", "employees", IOFormat.PARQUET), io.FileSource("clean/fake_employee_s/version=123/", "employeesss", IOFormat.PARQUET), io.FileSource("raw/hubspot/owners/", "owners", IOFormat.PARQUET), io.FileSource("clean/hubspot_owners/", "clean_owners", IOFormat.PARQUET)),
      List(new SQLFileMapping("src/test/resources/simple.sql", region)),
      io.FileSink("test", "src/test/tmp/gold/stripe_f_fake_employee_t/version=4/", SaveMode.Overwrite, IOFormat.PARQUET),
      Defaults(ConfigFactory.load()),
      Environment("", EngineMode.Batch, "", false, "test", "", Option(""), false, false,Seq("raw", "clean", "gold", "bronze"), Seq("raw_stripe", "raw_hubspot"))
    )

    val expectedJson =
      """{
        |  "entities": [
        |    {
        |      "typeName": "Process",
        |      "attributes": {
        |        "name": "test/raw_stripe_fake_employee,test/clean_fake_employee_s,test/raw_hubspot_owners,test/clean_hubspot_owners -> test/gold_stripe_f_fake_employee_t",
        |        "qualifiedName": "default/athena/1659962653/AwsDataCatalog/47b83f3425f72bfe7cbf3d966f9bda4b",
        |        "connectorName": "athena",
        |        "connectionName": "athena",
        |        "connectionQualifiedName": "default/athena/1659962653/AwsDataCatalog"
        |      },
        |      "relationshipAttributes": {
        |      "outputs": [
        |          {
        |            "typeName": "Table",
        |            "uniqueAttributes": {
        |              "qualifiedName": "default/athena/1659962653/AwsDataCatalog/test/gold_stripe_f_fake_employee_t"
        |            }
        |          }
        |        ],
        |        "inputs": [
        |          {
        |            "typeName": "Table",
        |            "uniqueAttributes": {
        |              "qualifiedName": "default/athena/1659962653/AwsDataCatalog/test/raw_stripe_fake_employee"
        |            }
        |          },          {
        |            "typeName": "Table",
        |            "uniqueAttributes": {
        |              "qualifiedName": "default/athena/1659962653/AwsDataCatalog/test/clean_fake_employee_s"
        |            }
        |          },          {
        |            "typeName": "Table",
        |            "uniqueAttributes": {
        |              "qualifiedName": "default/athena/1659962653/AwsDataCatalog/test/raw_hubspot_owners"
        |            }
        |          },          {
        |            "typeName": "Table",
        |            "uniqueAttributes": {
        |              "qualifiedName": "default/athena/1659962653/AwsDataCatalog/test/clean_hubspot_owners"
        |            }
        |          }
        |        ]
        |      }
        |    }
        |  ]
        |}""".stripMargin

    val calculatedJson = new AtlanService("foo")
      .generateBodyJson(testingConfig)
    print(calculatedJson)
    assert(expectedJson.trim.equalsIgnoreCase(calculatedJson.trim))
  }
}
