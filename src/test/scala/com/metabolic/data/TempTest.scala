package com.metabolic.data

import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp
import java.util.Calendar

class TempTest extends AnyFunSuite {

  test("tmp") {
    val now = Calendar.getInstance()
    now.set(Calendar.HOUR_OF_DAY, 0)
    now.set(Calendar.MINUTE, 0)
    now.set(Calendar.SECOND, 0)
    now.set(Calendar.MILLISECOND, 0)
    println(new Timestamp(now.getTimeInMillis))
  }

  test("tmp 2") {
    val now = Calendar.getInstance()
    now.add(Calendar.DAY_OF_YEAR, -1)
    println(new Timestamp(now.getTimeInMillis))
  }

}
