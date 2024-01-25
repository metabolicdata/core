package com.metabolic.data.core.services.spark.transform

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.core.services.spark.transformations.DemultiplexTransform
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp

class DemultiplexTest extends AnyFunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfterAll {

  test("Demultiplex by month") {
    val sqlCtx = sqlContext

    val inputData = Seq(
      Row("Marc", new Timestamp(1641026108000L)), // 1 enero
      Row("Marc", new Timestamp(1641803708000L)), // 10 de enero
      Row("Ausias", new Timestamp(1644482108000L)), // 10 de febrero
      Row("Ausias", new Timestamp(1645346108000L)), // 20 de febrero
      Row("Marc", new Timestamp(1647765308000L)) // 20 de marzo
    )

    val inputSchema = List(
      StructField("id", StringType, true),
      StructField("ts", TimestampType, true)
    )

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(inputSchema)
    )

    val outputDF = inputDF
      .transform(new DemultiplexTransform(Seq("id"), Seq("ts"), "ts", "Month")
        .demultiplex("2022-01-01", None, Some(false)))
      .where("period <= '2022-04-30'")

    val expectedData = Seq(
      Row("Marc", new Timestamp(1641803708000L), new Timestamp(1640991600000L)),   // 1 de enero
      Row("Marc", new Timestamp(1641803708000L), new Timestamp(1643670000000L)),   //1 de febrero que no existia
      Row("Marc", new Timestamp(1647765308000L), new Timestamp(1646089200000L)),   //1 marzo
      Row("Marc", new Timestamp(1647765308000L), new Timestamp(1648764000000L)),   //1 de abril que no existia
      Row("Ausias", new Timestamp(1645346108000L), new Timestamp(1643670000000L)), // 1 de febrero
      Row("Ausias", new Timestamp(1645346108000L), new Timestamp(1646089200000L)), //1 marzo que no existia
      Row("Ausias", new Timestamp(1645346108000L), new Timestamp(1648764000000L))  //1 de abril que no existia
    )

    val expectedSchema = List(
      StructField("id", StringType, true),
      StructField("ts", TimestampType, true),
      StructField("period", TimestampType, false)
    )
    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    assertDataFrameNoOrderEquals(outputDF, expectedDf)

  }

}
