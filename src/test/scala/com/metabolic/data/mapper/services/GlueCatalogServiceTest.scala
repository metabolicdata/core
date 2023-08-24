package com.metabolic.data.mapper.services

import com.metabolic.data.RegionedTest
import com.metabolic.data.core.domain.Environment
import com.metabolic.data.core.services.glue.GlueCrawlerService
import com.metabolic.data.mapper.domain.io.EngineMode
import org.scalatest.funsuite.AnyFunSuite


class GlueCatalogServiceTest extends AnyFunSuite
  with RegionedTest {

  ignore("Works") {

    val s3Path = Seq("s3://factorial-etl/entity_mapper/dl/clean/subs/")
    val config = Environment("[Test] ", EngineMode.Batch, "", true, "test_data_lake", "AWSGlueServiceRoleDefault", Option.apply("fooBarAtlan"), Option.apply("fooBarAtlan"))

    //GlueCatalogService.register(config, "subsy", s3Path, "clean_")

    new GlueCrawlerService().runCrawler("subsy")
    val metrics = new GlueCrawlerService().checkStatus("subsy")

    assert(metrics != null)

  }
}
