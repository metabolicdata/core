package com.metabolic.data.mapper.services

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.RegionedTest
import com.metabolic.data.core.domain.{Defaults, Environment}
import com.metabolic.data.core.services.catalogue.AtlanService
import com.metabolic.data.core.services.util.{ConfigReaderService, ConfigUtilsService}
import com.metabolic.data.mapper.domain._
import com.metabolic.data.mapper.domain.io.{EngineMode, IOFormat, WriteMode}
import com.metabolic.data.mapper.domain.ops.SQLFileMapping
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SaveMode
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

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
      io.FileSink("test", "src/test/tmp/gold/stripe_f_fake_employee_t/version=4/", WriteMode.Overwrite, IOFormat.PARQUET),
      Defaults(ConfigFactory.load()),
      Environment("", EngineMode.Batch, "", false, "test", "", region, Option(""), Option(""), Option(""), false, false,Seq("raw", "clean", "gold", "bronze"), Seq("raw_stripe", "raw_hubspot"))
    )

    val expectedJson =
      """{
        |  "entities": [
        |    {
        |      "typeName": "Process",
        |      "attributes": {
        |        "name": "test/raw_stripe_fake_employee,test/clean_fake_employee_s,test/raw_hubspot_owners,test/clean_hubspot_owners -> test/gold_stripe_f_fake_employee_t",
        |        "qualifiedName": "foo47b83f3425f72bfe7cbf3d966f9bda4b",
        |        "connectorName": "athena",
        |        "connectionName": "athena",
        |        "connectionQualifiedName": "fo"
        |      },
        |      "relationshipAttributes": {
        |        "outputs": [
        |          {
        |            "typeName": "Table",
        |            "uniqueAttributes": {
        |              "qualifiedName": "footest/gold_stripe_f_fake_employee_t"
        |            }
        |          }
        |        ],
        |        "inputs": [
        |
        |          {
        |            "typeName": "Table",
        |            "uniqueAttributes": {
        |              "qualifiedName": "footest/raw_stripe_fake_employee"
        |            }
        |          },
        |          {
        |            "typeName": "Table",
        |            "uniqueAttributes": {
        |              "qualifiedName": "footest/clean_fake_employee_s"
        |            }
        |          },
        |          {
        |            "typeName": "Table",
        |            "uniqueAttributes": {
        |              "qualifiedName": "footest/raw_hubspot_owners"
        |            }
        |          },
        |          {
        |            "typeName": "Table",
        |            "uniqueAttributes": {
        |              "qualifiedName": "footest/clean_hubspot_owners"
        |            }
        |          }
        |        ]
        |      }
        |    }
        |  ]
        |}""".stripMargin

    val calculatedJson = new AtlanService("foo", "foo", "foo")
      .generateBodyJson(testingConfig)
    print(calculatedJson)
    assert(expectedJson.trim.equalsIgnoreCase(calculatedJson.trim))
  }

  test("Test generated json for lineage Confluent") {

    val testingConfig = Config(
      "",
      List(io.StreamSource("Name test",Seq("test"), "test", "test", "topic1", IOFormat.KAFKA), io.FileSource("raw/stripe/fake_employee/version=3/", "employees", IOFormat.PARQUET), io.FileSource("clean/fake_employee_s/version=123/", "employeesss", IOFormat.PARQUET), io.FileSource("raw/hubspot/owners/", "owners", IOFormat.PARQUET), io.FileSource("clean/hubspot_owners/", "clean_owners", IOFormat.PARQUET)),
      List(new SQLFileMapping("src/test/resources/simple.sql", region)),
      io.StreamSink("Name test",Seq("test"), "test", "test", "topic2", Option.empty, IOFormat.KAFKA, Seq.empty),
      Defaults(ConfigFactory.load()),
      Environment("", EngineMode.Batch, "", false, "test", "", region, Option(""), Option(""), Option(""), false, false,Seq("raw", "clean", "gold", "bronze"), Seq("raw_stripe", "raw_hubspot"))
    )

    val expectedJson =
      """{
        |  "entities": [
        |    {
        |      "typeName": "Process",
        |      "attributes": {
        |        "name": "topic1,test/raw_stripe_fake_employee,test/clean_fake_employee_s,test/raw_hubspot_owners,test/clean_hubspot_owners -> topic2",
        |        "qualifiedName": "fooc2875cff74a12d49ebaa2c5f82228063",
        |        "connectorName": "confluent-kafka",
        |        "connectionName": "production",
        |        "connectionQualifiedName": "fo"
        |      },
        |      "relationshipAttributes": {
        |        "outputs": [
        |          {
        |            "typeName": "KafkaTopic",
        |            "uniqueAttributes": {
        |              "qualifiedName": "footopic2"
        |            }
        |          }
        |        ],
        |        "inputs": [
        |
        |          {
        |            "typeName": "KafkaTopic",
        |            "uniqueAttributes": {
        |              "qualifiedName": "footopic1"
        |            }
        |          },
        |          {
        |            "typeName": "Table",
        |            "uniqueAttributes": {
        |              "qualifiedName": "footest/raw_stripe_fake_employee"
        |            }
        |          },
        |          {
        |            "typeName": "Table",
        |            "uniqueAttributes": {
        |              "qualifiedName": "footest/clean_fake_employee_s"
        |            }
        |          },
        |          {
        |            "typeName": "Table",
        |            "uniqueAttributes": {
        |              "qualifiedName": "footest/raw_hubspot_owners"
        |            }
        |          },
        |          {
        |            "typeName": "Table",
        |            "uniqueAttributes": {
        |              "qualifiedName": "footest/clean_hubspot_owners"
        |            }
        |          }
        |        ]
        |      }
        |    }
        |  ]
        |}""".stripMargin

    val calculatedJson = new AtlanService("foo", "foo", "foo")
      .generateBodyJson(testingConfig)
    print(calculatedJson)
    assert(expectedJson.trim.equalsIgnoreCase(calculatedJson.trim))
  }

  test("Test fake asset GUI - should not stop execution") {
    val response = new AtlanService("foo", "foo", "foo")
      .getGUI("test")
    assert(response.trim.equalsIgnoreCase(""))
  }

  ignore("Test metadata body") {
    val testingConfig = Config(
      "",
      List(io.FileSource("raw/stripe/fake_employee/version=3/", "employees", IOFormat.PARQUET), io.FileSource("clean/fake_employee_s/version=123/", "employeesss", IOFormat.PARQUET), io.FileSource("raw/hubspot/owners/", "owners", IOFormat.PARQUET), io.FileSource("clean/hubspot_owners/", "clean_owners", IOFormat.PARQUET)),
      List(new SQLFileMapping("src/test/resources/simple.sql", region)),
      io.FileSink("test", "src/test/tmp/gold/stripe_f_fake_employee_t/version=4/", WriteMode.Overwrite, IOFormat.PARQUET),
      Defaults(ConfigFactory.load()),
      Environment("", EngineMode.Batch, "", false, "test", "", region, Option(""),Option(""),  Option(""), false, false, Seq("raw", "clean", "gold", "bronze"), Seq("raw_stripe", "raw_hubspot"))
    )
    val calculatedJson = new AtlanService("foo", "foo", "foo")
      .generateMetadaBody(testingConfig)

    val expectedJson =
      s"""
        |{
        |  "Data Quality": {
        |    "last_synced_at" : ${LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))},
        |    "engine_type":"batch",
        |    "sql_mapping":"select * from employees where age < 40"
        |  }
        |}
        |""".stripMargin
    assert(expectedJson.trim.equalsIgnoreCase(calculatedJson.trim))
  }

  test("Test generated version for owner") {

    val rawConfig = new ConfigReaderService().getConfig("src/test/resources/employees.conf")

    val config = new ConfigParserService()
      .parseConfig(rawConfig)

    val calculatedJson = new AtlanService("foo", "foo", "foo")
      .generateOwnerBody(config.head)

    val expectedJson = {
      s"""
         |{
         |  "owners": ["Fernando Azpiazu"]
         |}
         |""".stripMargin
    }

    assert(expectedJson.trim.equalsIgnoreCase(calculatedJson.trim))
  }

  test("Test generated version for SQL and Conf resources") {

    val rawConfig = new ConfigReaderService().getConfig("src/test/resources/employees.conf")

    val config = new ConfigParserService()
      .parseConfig(rawConfig)

    val calculatedJson = new AtlanService("foo", "foo", "foo")
      .generateResourceBody(config.head)

    val expectedJson = {
      s"""
         |{
         |  "resources": [
         |    {
         |      "name": "SQL File",
         |      "type": "LINK",
         |      "url": "src/test/resources/employees.sql"
         |    },
         |    {
         |      "name": "Conf File",
         |      "type": "LINK",
         |      "url": "src/test/resources/employees.conf"
         |    }
         |  ]
         |}
         |""".stripMargin
    }

    assert(expectedJson.trim.equalsIgnoreCase(calculatedJson.trim))
  }
}
