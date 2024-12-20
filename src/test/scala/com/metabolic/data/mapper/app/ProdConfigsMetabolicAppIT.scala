package com.metabolic.data.mapper.app

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.RegionedTest
import com.metabolic.data.core.services.util.ConfigReaderService
import com.metabolic.data.mapper.domain.run.EngineMode
import com.metabolic.data.mapper.services.ConfigParserService
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class ProdConfigsMetabolicAppIT extends AnyFunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfterAll
  with RegionedTest {

  ignore("ana") {

    val config = new ConfigReaderService()
      .getConfig("src/test/resources/prod.config_examples/in_batch_read_kafka_map_star_write_parquet.conf")

    val parsedConfig = new ConfigParserService()
      .parseConfig(config)

    MetabolicApp()
      .transformAll(parsedConfig)(region, spark)

  }

  ignore("ausias") {

    val config = new ConfigReaderService()
      .getConfig("src/test/resources/prod.config_examples/in_batch_read_table.conf")

    val parsedConfig = new ConfigParserService()
      .parseConfig(config)

    parsedConfig.head.sources.foreach { source =>
      MetabolicReader.read(source, true, EngineMode.Batch, false, "", "")(spark)
    }


  }



}
