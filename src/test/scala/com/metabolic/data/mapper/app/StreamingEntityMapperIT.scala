package com.metabolic.data.mapper.app

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.RegionedTest
import com.metabolic.data.core.services.util.ConfigReaderService
import com.metabolic.data.mapper.services.ConfigParserService
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class StreamingEntityMapperIT extends AnyFunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfterAll
  with RegionedTest {

  ignore("Read kafka from CC") {

    val sqlCtx = sqlContext

    val rawConfig = new ConfigReaderService()
      .getConfig("src/test/resources/com.metabolic.data.em.services/sampleconfigs/kafka.conf")

    val testingConfig = new ConfigParserService()
      .parseConfig(rawConfig)

    MetabolicApp()
      .transformAll(testingConfig)(region,spark)

  }

  ignore("Write to CC kafka") {

    val sqlCtx = sqlContext

    val rawConfig = new ConfigReaderService()
      .getConfig("src/test/resources/prod.config_examples/in_batch_read_kafka_map_star_write_parquet.conf")

    val testingConfig = new ConfigParserService()
      .parseConfig(rawConfig)

    MetabolicApp()
      .transformAll(testingConfig)(region,spark)

  }
}
