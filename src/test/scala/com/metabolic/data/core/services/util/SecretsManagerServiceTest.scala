package com.metabolic.data.core.services.util

import com.metabolic.data.RegionedTest
import com.metabolic.data.core.domain.DDBBConnection
import org.scalatest.funsuite.AnyFunSuite

class SecretsManagerServiceTest extends AnyFunSuite with RegionedTest  {

  test("Works") {
    val x = new SecretsManagerService()
      .get("production/database_credentials")

    val ddbbConfig = new SecretsManagerService()
      .parseDict[DDBBConnection](x)

    val database = ddbbConfig.dbname
    val engine = ddbbConfig.engine
    val host = ddbbConfig.read_replica_host
    val port = ddbbConfig.port
    val user = ddbbConfig.username
    val pass = ddbbConfig.password

    val jdbcUrl = s"jdbc:$engine://$host:$port/$database"

    println(jdbcUrl)
    println(user)
    println(pass)

  }

}
