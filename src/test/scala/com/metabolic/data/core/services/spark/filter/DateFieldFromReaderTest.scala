package com.metabolic.data.core.services.spark.filter

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.joda.time.DateTime
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp

class DateFieldFromReaderTest extends AnyFunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfterAll{

  test("Filters < 2021 rows out") {
    val sqlCtx = sqlContext

    val inputData = Seq(
      Row("Marc", new Timestamp(1609488963000L)),
      Row("Pau", new Timestamp(1577866563000L)),
      Row("Enric", new Timestamp(1546330563000L))
    )

    val someSchema = List(
      StructField("name", StringType, true),
      StructField("ts", TimestampType, true)
    )

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(someSchema)
    )

    val outputDF = inputDF
      .transform(new DateFieldFromReader("ts", new DateTime(new Timestamp(1590999363000L).getTime)).filter())

    val expectedData = Seq(
      Row("Marc", new Timestamp(1609488963000L))
    )

    val expextedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(someSchema)
    )

    assertDataFrameNoOrderEquals(outputDF, expextedDF)

  }

}
