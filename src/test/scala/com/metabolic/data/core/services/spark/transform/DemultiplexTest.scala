package com.metabolic.data.core.services.spark.transform

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.core.services.spark.transformations.DemultiplexTransform
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.not
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
      Row("Marc", Timestamp.valueOf("2022-01-01 09:35:08")), // 1 enero
      Row("Marc", Timestamp.valueOf("2022-01-10 09:35:08")), // 10 de enero
      Row("Ausias", Timestamp.valueOf("2022-02-10 09:35:08")), // 10 de febrero
      Row("Ausias", Timestamp.valueOf("2022-02-20 09:35:08")), // 20 de febrero
      Row("Marc", Timestamp.valueOf("2022-03-20 09:35:08")) // 20 de marzo
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
      Row("Marc", Timestamp.valueOf("2022-01-10 09:35:08"), Timestamp.valueOf("2022-01-01 00:00:00")),   // 1 de enero
      Row("Marc", Timestamp.valueOf("2022-01-10 09:35:08"), Timestamp.valueOf("2022-02-01 00:00:00")),   //1 de febrero que no existia
      Row("Marc", Timestamp.valueOf("2022-03-20 09:35:08"), Timestamp.valueOf("2022-03-01 00:00:00")),   //1 marzo
      Row("Marc", Timestamp.valueOf("2022-03-20 09:35:08"), Timestamp.valueOf("2022-04-01 00:00:00")),   //1 de abril que no existia
      Row("Ausias", Timestamp.valueOf("2022-02-20 09:35:08"), Timestamp.valueOf("2022-02-01 00:00:00")), // 1 de febrero
      Row("Ausias", Timestamp.valueOf("2022-02-20 09:35:08"), Timestamp.valueOf("2022-03-01 00:00:00")), //1 marzo que no existia
      Row("Ausias", Timestamp.valueOf("2022-02-20 09:35:08"), Timestamp.valueOf("2022-04-01 00:00:00"))  //1 de abril que no existia
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

    assertDataFrameNoOrderEquals(outputDF,expectedDf)


  }

}
