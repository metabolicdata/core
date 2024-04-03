package com.metabolic.data.mapper.services

import com.metabolic.data.RegionedTest
import com.metabolic.data.core.services.util.{ConfigReaderService, DefaultsUtilsService}
import com.metabolic.data.mapper.domain.io.{FileSink, FileSource, IOFormat}
import com.metabolic.data.mapper.domain.ops._
import com.metabolic.data.mapper.domain.ops.mapping.TupletIntervalMapping
import com.metabolic.data.mapper.domain.ops.source.{DedupeSourceOp, FilterSourceOp, SQLOrder, SelectExpressionSourceOp}
import org.scalatest.funsuite.AnyFunSuite

import scala.util.matching.Regex

class ConfigReaderServiceTest extends AnyFunSuite with RegionedTest  {

  val personHOCON =
    """name = "Bob"
      |salary = 30000
      |sources: [
      | { ios = "13"},
      | { android = "kitkat"}
      |]
      |""".stripMargin

  test("ConfigReaderService with blank path") {
    val overrideConfig = new ConfigReaderService()
      .getConfig("")
    assert(overrideConfig.getString("source.format") == "json")
  }

  test("ConfigReaderService resolves app config content") {

    val overrideConfig = new ConfigReaderService().loadConfig(personHOCON, "")

    assert(overrideConfig.getString("name") == "Bob")
    assert(overrideConfig.getInt("age") == 33)

  }

  test("ConfigReaderService resolves system variable") {

    System.setProperty("sp.region", "eu-west-3")

    val overrideConfig = new ConfigReaderService().loadConfig(personHOCON, "")

    assert(overrideConfig.getString("name") == "Bob")
    assert(overrideConfig.getInt("age") == 33)
    assert(overrideConfig.getConfig("dp").getString("region") == "eu-west-3")

  }

  test("ConfigReaderService resolves arg map") {

    val args: Map[String, String] = Map(
      "dp.region" -> "eu-west-3",
      "dp.bucket" -> "testy"
    )

    val url = "https://raw.githubusercontent.com/lightbend/config/main/examples/scala/simple-lib/src/main/resources/reference.conf"

    val overrideConfig = new ConfigReaderService()
      .getConfig(url, args)//args)

    assert(overrideConfig.getString("simple-lib.foo") == "This value comes from simple-lib's reference.conf")
    assert(overrideConfig.getString("dp.region") == "eu-west-3")

  }

  test("ConfigReaderService resolves multiple objects") {

    val overrideConfig = new ConfigReaderService().loadConfig(personHOCON, "")

    val sources = overrideConfig.getConfigList("sources")
    assert( sources.size().equals(2))
    assert( sources.get(0).getString("ios") == "13")

  }

  test("ConfigReaderService resolves run config file ") {

    val overrideConfig = new ConfigReaderService().getConfig("src/test/resources/run.conf")

    assert(overrideConfig.getString("name") == "Enric Job")

  }

  test("ConfigReaderService resolves a simple configuraion") {

    val overrideConfig = new ConfigReaderService()
      .getConfig("src/test/resources/com.metabolic.data.em.services/sampleconfigs/simple.conf")

    assert(overrideConfig.getString("mapping.file") == "src/test/resources/employees.sql")

    val parsedOverrideConfig = new ConfigParserService().parseConfig(overrideConfig)(0)

    val contents = parsedOverrideConfig.mappings(0).asInstanceOf[SQLMapping].sqlContents

    assert(contents == "select employees.name as employee_name, age, companies.name as company_name" +
      " from employees join companies ON employees.company = companies.id where age < 40")

    val firstFormat = parsedOverrideConfig.sources(0).asInstanceOf[FileSource].format
    val secondFormat = parsedOverrideConfig.sources(1).asInstanceOf[FileSource].format

    assert(firstFormat == IOFormat.JSON)
    assert(secondFormat == IOFormat.PARQUET)

  }

  test("ConfigReaderService resolves a complex configuration") {

    val overrideConfig = new ConfigReaderService()
      .getConfig("src/test/resources/com.metabolic.data.em.services/sampleconfigs/multiple.conf")

    val parsedOverrideConfig = new ConfigParserService().parseConfig(overrideConfig)

    val firstEntity = parsedOverrideConfig(0)
    val secondEntity = parsedOverrideConfig(1)
    var thirdEntity = parsedOverrideConfig(2)

    val contents = firstEntity.mappings(0).asInstanceOf[SQLMapping].sqlContents

    assert(contents == "select employees.name as employee_name, age, companies.name as company_name" +
      " from employees join companies ON employees.company = companies.id where age < 40")

    val firstFormat = firstEntity.sources(0).asInstanceOf[FileSource].format

    assert(firstFormat == IOFormat.JSON)

    val secondFormat = secondEntity.sources(0).asInstanceOf[FileSource].format

    assert(secondFormat == IOFormat.DELTA)

    val secondUpsert = secondEntity.sink.asInstanceOf[FileSink].upsert

    assert(secondUpsert == false)

    val thirdUpsert = thirdEntity.sink.asInstanceOf[FileSink].upsert

    assert(thirdUpsert == true)

  }

  test("ConfigReaderService resolves a variable ") {

    val overrideConfig = new ConfigReaderService().getConfig("src/test/resources/run_variable.conf")

    assert(overrideConfig.getString("path") == "s3://factorial-dl-raw/tmp")

  }

  test("ConfigReaderService resolves a preload ") {

    val overrideConfig = new ConfigReaderService().getConfig("src/test/resources/run_variable.conf")
    val datePattern: String = """\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"""

    assert(overrideConfig.getString("time").matches(datePattern))
  }

  test("ConfigReaderService resolves a variable in map ") {

    val args: Map[String, String] = Map(
      "dp.region" -> "eu-west-1",
      "dp.raw_bucket" -> "s3://factorial-dl-rawy",
      "dp.time" -> "2021-01-01T00:00:00"
    )

    val overrideConfig = new ConfigReaderService().getConfig("src/test/resources/run_map.conf", args)

    assert(overrideConfig.getString("pathy") == "s3://factorial-dl-rawy/tmp")

  }

  test("ConfigReaderService resolves a preload in a map") {

    val args: Map[String, String] = Map(
      "dp.region" -> "eu-west-1",
      "dp.raw_bucket" -> "s3://factorial-dl-rawy",
      "dp.time" -> "df.now"
    )

    val overrideConfig = new ConfigReaderService().getConfig("src/test/resources/run_map.conf", args)
    val datePattern: String = """\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"""

    assert(overrideConfig.getString("time").matches(datePattern))
  }

  test("ConfigReaderService resolves mappings ") {

    val config = new ConfigReaderService()
      .getConfig("src/test/resources/com.metabolic.data.em.services/sampleconfigs/multiple_mappings.conf")

    val parsedConfig = new ConfigParserService().parseConfig(config)

    val postOp = parsedConfig(0).mappings(0).asInstanceOf[TupletIntervalMapping]

    assert(postOp.leftTableName == "factorial_clean_companies")
    assert(postOp.rightTableName == "factorial_raw_subscriptions")
    assert(postOp.leftIdColumnName == "id")
    assert(postOp.rightIdColumnName == "id")
    assert(postOp.leftWindowColumnName == "updated_at")
    assert(postOp.rightWindowColumnName == "x_updated_at")

  }

  test("ConfigReaderService resolves new sources ") {

    val config = new ConfigReaderService()
      .getConfig("src/test/resources/com.metabolic.data.em.services/sampleconfigs/new_sources.conf")

    val parsedConfig = new ConfigParserService().parseConfig(config)

    val fitlerSourceOp = parsedConfig(0).sources(0).ops(0).asInstanceOf[FilterSourceOp]
    assert(fitlerSourceOp.fromDate == DefaultsUtilsService.getDateTime("start_of_yesterday", config.getConfig("df")))
    assert(fitlerSourceOp.toDate == DefaultsUtilsService.getDateTime("now", config.getConfig("df")))

    assert(fitlerSourceOp.fromDate == parsedConfig(0).defaults.getDateTime("start_of_yesterday"))
    assert(fitlerSourceOp.toDate == parsedConfig(0).defaults.getDateTime("now"))

    assert(fitlerSourceOp.onColumn == "updated_at")

    val dedupeSourceOp = parsedConfig(0).sources(0).ops(2).asInstanceOf[DedupeSourceOp]

    assert(dedupeSourceOp.idColumns == Seq("id"))
    assert(dedupeSourceOp.orderColumns == Seq("yyyy","mm","dd","updated_at", "extracted_at"))
    assert(dedupeSourceOp.order == SQLOrder.Ascending)

    val exprSourceOp = parsedConfig(0).sources(1).ops(1).asInstanceOf[SelectExpressionSourceOp]

    assert(exprSourceOp.expressions(1) == "capital(id) as c_id")

  }

  //Mock with Mockito AWSSecretManager
  ignore("ConfigReaderService resolves streaming mapping ") {

    val config = new ConfigReaderService()
      .getConfig("src/test/resources/com.metabolic.data.em.services/sampleconfigs/streaming.conf")

    val parsedConfig = new ConfigParserService().parseConfig(config)

    assert(parsedConfig(0).sources(0).name == "factorial_gold_employees")
  }

  test("ConfigReaderService resolves table ") {

    val config = new ConfigReaderService()
      .getConfig("src/test/resources/com.metabolic.data.em.services/sampleconfigs/new_sources.conf")

    val parsedConfig = new ConfigParserService().parseConfig(config)

    val fitlerSourceOp = parsedConfig(0).sources(0).ops(0).asInstanceOf[FilterSourceOp]
    assert(fitlerSourceOp.fromDate == DefaultsUtilsService.getDateTime("start_of_yesterday", config.getConfig("df")))
    assert(fitlerSourceOp.toDate == DefaultsUtilsService.getDateTime("now", config.getConfig("df")))

    assert(fitlerSourceOp.fromDate == parsedConfig(0).defaults.getDateTime("start_of_yesterday"))
    assert(fitlerSourceOp.toDate == parsedConfig(0).defaults.getDateTime("now"))

    assert(fitlerSourceOp.onColumn == "updated_at")

    val dedupeSourceOp = parsedConfig(0).sources(0).ops(2).asInstanceOf[DedupeSourceOp]

    assert(dedupeSourceOp.idColumns == Seq("id"))
    assert(dedupeSourceOp.orderColumns == Seq("yyyy","mm","dd","updated_at", "extracted_at"))
    assert(dedupeSourceOp.order == SQLOrder.Ascending)

    val exprSourceOp = parsedConfig(0).sources(1).ops(1).asInstanceOf[SelectExpressionSourceOp]

    assert(exprSourceOp.expressions(1) == "capital(id) as c_id")

  }

  ignore("ConfigReaderService resolves streaming attendance ") {

    val config = new ConfigReaderService()
      .getConfig("src/test/resources/com.metabolic.data.em.services/sampleconfigs/streaming_attendance.conf")

    val parsedConfig = new ConfigParserService().parseConfig(config)

    assert(parsedConfig(0).sources(0).name == "factorial_gold_employees")
  }


}
