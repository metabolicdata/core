package com.metabolic.data.core.services.spark.interval

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp

class IntervalReaderTest extends AnyFunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfterAll {

    test("Generate Intervals of two entities") {

        val leftData = Seq(
            Row("1", new Timestamp(1609459200000L), "10"),
            Row("1", new Timestamp(1609632000000L), "13"),
            Row("2", new Timestamp(1609632000000L), "20"),
            Row("2", new Timestamp(1609675200000L), "22")
        )

        val leftSchema = List(
            StructField("l_id", StringType, true),
            StructField("l_ts", TimestampType, true),
            StructField("value", StringType, true)
        )

        val leftDF = spark.createDataFrame(
            spark.sparkContext.parallelize(leftData),
            StructType(leftSchema)
        )

        val rightData = Seq(
            Row("1", new Timestamp(1609459200000L), "5"),
            Row("1", new Timestamp(1609545600000L), "8"),
            Row("2", new Timestamp(1609545600000L), "3")
        )

        val rightSchema = List(
            StructField("r_id", StringType, true),
            StructField("r_ts", TimestampType, true),
            StructField("feature", StringType, true)
        )

        val rightDF = spark.createDataFrame(
            spark.sparkContext.parallelize(rightData),
            StructType(rightSchema)
        )


        /// *******

        val intervals = new IntervalReader(leftDF,"l_id", "l_ts" )
          .generate( rightDF, "r_id", "r_ts")
          .withColumnRenamed("l_id", "id").drop(("r_id"))


        /// *******

        val expectedData = Seq(
            Row("1", new Timestamp(1609459200000L), new Timestamp(1609459200000L), "10", "5"),
            Row("1", new Timestamp(1609459200000L), new Timestamp(1609545600000L), "10", "8"),
            Row("1", new Timestamp(1609632000000L), new Timestamp(1609545600000L), "13", "8"),
            Row("2", new Timestamp(1609632000000L), new Timestamp(1609545600000L), "20", "3"),
            Row("2", new Timestamp(1609675200000L), new Timestamp(1609545600000L), "22", "3")
        )

        val expectedSchema = List(
            StructField("id", StringType, true),
            StructField("l_ts", TimestampType, true),
            StructField("r_ts", TimestampType, true),
            StructField("value", StringType, true),
            StructField("feature", StringType, true)
        )

        val expextedDF = spark.createDataFrame(
            spark.sparkContext.parallelize(expectedData),
            StructType(expectedSchema)
        )

        val output = intervals
          .selectExpr("id", "l_ts", "r_ts","value", "feature")
          .orderBy("id", "l_ts")

        output.show()
        expextedDF.show()
        assertDataFrameNoOrderEquals(output, expextedDF)

    }

    test("Generate Bigger Intervals of two entities") {

        val squareData = Seq(
            Row("2", new Timestamp(1607126400000L), "9"),
            Row("1", new Timestamp(1609459200000L), "10"),
            Row("1", new Timestamp(1609632000000L), "13"),
            Row("2", new Timestamp(1609632000000L), "20"),
            Row("2", new Timestamp(1609675200000L), "22"),
            Row("1", new Timestamp(1612483200000L), "XX")
        )

        val squareSchema = List(
            StructField("square_id", StringType, true),
            StructField("square_ts", TimestampType, true),
            StructField("length", StringType, true)
        )

        val square = spark.createDataFrame(
            spark.sparkContext.parallelize(squareData),
            StructType(squareSchema)
        ).where("square_id = '2'")

        val circleData = Seq(
            Row("1", new Timestamp(1609459200000L), "5"),
            Row("1", new Timestamp(1609545600000L), "8"),
            Row("2", new Timestamp(1609545600000L), "3"),
            Row("1", new Timestamp(1609675200000L), "19"),
            Row("2", new Timestamp(1609675200000L), "18")
        )

        val circleSchema = List(
            StructField("circle_id", StringType, true),
            StructField("circle_ts", TimestampType, true),
            StructField("radius", StringType, true)
        )

        val circle = spark.createDataFrame(
            spark.sparkContext.parallelize(circleData),
            StructType(circleSchema)
        ).where("circle_id = '2'")


        /// *******

        val intervals = new IntervalReader(square, "square_id", "square_ts")
          .generate( circle, "circle_id", "circle_ts")
          .withColumnRenamed("square_id", "id").drop(("square_id"))


        /// *******

        val expectedData = Seq(
            Row("2", new Timestamp(1607126400000L), new Timestamp(1609545600000L), "9", "3"),
            Row("2", new Timestamp(1609632000000L), new Timestamp(1609545600000L), "20", "3"),
            Row("2", new Timestamp(1609675200000L), new Timestamp(1609675200000L), "22", "18")
        )

        val expectedSchema = List(
            StructField("id", StringType, true),
            StructField("square_ts", TimestampType, true),
            StructField("circle_ts", TimestampType, true),
            StructField("length", StringType, true),
            StructField("radius", StringType, true)
        )

        val expectedDF = spark.createDataFrame(
            spark.sparkContext.parallelize(expectedData),
            StructType(expectedSchema)
        )

        val output = intervals
          .selectExpr("id", "square_ts", "circle_ts", "length", "radius")

        assertDataFrameNoOrderEquals(output, expectedDF)

    }


}
