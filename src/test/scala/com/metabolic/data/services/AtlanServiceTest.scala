package com.metabolic.data.services

import com.metabolic.data.RegionedTest
import com.metabolic.data.core.services.util.ConfigReaderService
import org.scalatest.funsuite.AnyFunSuite

class AtlanServiceTest extends AnyFunSuite with RegionedTest  {

  test("atlanCatalogServiceTest") {

    val configService = new ConfigReaderService()

    // Load config file from path
    val config = configService.getConfig("src/test/resources/employees.conf")

    asset(config != null)
}

