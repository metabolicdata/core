package com.metabolic.data.core.services.spark.udfs

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class UDFTransformationTest extends AnyFunSuite
  with DataFrameSuiteBase
  with SharedSparkContext
  with BeforeAndAfterAll {

    case class DummyUDF() extends MetabolicUserDefinedFunction {
        override def name(): String = "dummy_udf"

        def transformation: UserDefinedFunction = udf((s: String) => {
            "dummy " + s
        })

    }

    test("Simple UDF test") {

        // id, fruit, color
         val inputEventsData = Seq(
             Row("1","apple","red"),
             Row("2","banana","yellow"),
             Row("3","orange","orange"),
             Row("2","banana","yellow"),
             Row("5","watermelon","green"),
             Row("6","strawberry","red"),
             Row("7","kiwi","green"),
             Row("8","pineapple","yellow"),
             Row("8","pineapple","yellow"),
             Row("10","mango","orange")
         )

        val inputSchema = List(
            StructField("id", StringType, true),
            StructField("fruit", StringType, true),
            StructField("color", StringType, true)
        )

        val inputEventsDF = spark.createDataFrame(
            spark.sparkContext.parallelize(inputEventsData),
            StructType(inputSchema)
        )

        inputEventsDF.createOrReplaceTempView("fruits")


        val dummy_udf = DummyUDF()
        spark.udf.register(dummy_udf.name(), dummy_udf.transformation)
        val result = spark.sql("""SELECT id, fruit, dummy_udf(color) as dummy_color FROM fruits""")


        val expectedEventsData = Seq(
            Row("1", "apple", "dummy red"),
            Row("2", "banana", "dummy yellow"),
            Row("3", "orange", "dummy orange"),
            Row("2", "banana", "dummy yellow"),
            Row("5", "watermelon", "dummy green"),
            Row("6", "strawberry", "dummy red"),
            Row("7", "kiwi", "dummy green"),
            Row("8", "pineapple", "dummy yellow"),
            Row("8", "pineapple", "dummy yellow"),
            Row("10", "mango", "dummy orange")
        )

        val expectedSchema = List(
            StructField("id", StringType, true),
            StructField("fruit", StringType, true),
            StructField("dummy_color", StringType, true)
        )

        val expectedEventsDF = spark.createDataFrame(
            spark.sparkContext.parallelize(expectedEventsData),
            StructType(expectedSchema)
        )
        
        assertDataFrameEquals(result, expectedEventsDF)


    }

}
