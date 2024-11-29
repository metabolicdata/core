package com.metabolic.data.services

import com.metabolic.data.RegionedTest
import com.metabolic.data.core.services.util.ConfigReaderService
import org.scalatest.funsuite.AnyFunSuite

class AtlanServiceTest extends AnyFunSuite with RegionedTest  {

  test("testPreloadConfig") {

    val configService = new ConfigReaderService()

    // Load config file from path
    val config = configService.getConfig("src/test/resources/employees.conf")

    // When file in config.mapping
    val fileUrl = if (config.hasPath("mapping.file")) {
      config.getConfig("mapping").getString("file")
    } else {
      ""
    }

    // assert variable fileUrl is not ""
    assert(fileUrl != "")
  }
}

