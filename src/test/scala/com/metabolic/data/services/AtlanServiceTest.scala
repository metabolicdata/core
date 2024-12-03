package com.metabolic.data.services

import com.metabolic.data.RegionedTest
import com.metabolic.data.core.services.util.ConfigReaderService
import com.metabolic.data.mapper.services.ConfigParserService
import org.scalatest.funsuite.AnyFunSuite

class AtlanServiceTest extends AnyFunSuite with RegionedTest  {

  test("atlanCatalogServiceTest") {

    val rawConfig = new ConfigReaderService().getConfig("src/test/resources/employees.conf")

    val config = new ConfigParserService()
      .parseConfig(rawConfig)

    val owner = config.head.owner
    val sqlFile = config.head.sqlUrl
    val confFile = config.head.confUrl

    assert(owner != "")
    assert(sqlFile != "")
    assert(confFile != "")
  }
}

