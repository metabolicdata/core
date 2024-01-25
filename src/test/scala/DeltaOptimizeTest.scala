import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.RegionedTest
import com.metabolic.data.core.services.spark.writer.file.DeltaWriter
import com.metabolic.data.mapper.app.MetabolicApp
import com.metabolic.data.mapper.domain.io.{EngineMode, WriteMode}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import io.delta.tables._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class DeltaOptimizeTest
  extends AnyFunSuite
    with DataFrameSuiteBase
    with SharedSparkContext
    with RegionedTest {


  override def conf: SparkConf = super.conf
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .set("spark.databricks.delta.retentionDurationCheck.enabled","false")


  val inputData = Seq(
    Row("A", "a", 2022, 2, 5, "2022-02-05"),
    Row("B", "b", 2022, 2, 4, "2022-02-04"),
    Row("C", "c", 2022, 2, 3, "2022-02-03"),
    Row("D", "d", 2022, 2, 2, "2022-02-02"),
    Row("E", "e", 2022, 2, 1, "2022-02-01"),
    Row("F", "f", 2022, 1, 5, "2022-01-05"),
    Row("G", "g", 2021, 2, 2, "2021-02-02"),
    Row("H", "h", 2020, 2, 5, "2020-02-05")
  )

  val someSchema = List(
    StructField("name", StringType, true),
    StructField("data", StringType, true),
    StructField("yyyy", IntegerType, true),
    StructField("mm", IntegerType, true),
    StructField("dd", IntegerType, true),
    StructField("date", StringType, true),
  )

  ignore("Delta Optimize") {

    val pathToTable = "src/test/tmp/delta/letters_overwrite"

    import io.delta.implicits._

    val deltaTable = DeltaTable.forPath(spark, pathToTable) // For path-based tables
    // For Hive metastore-based tables: val deltaTable = DeltaTable.forName(spark, tableName)

//    val start_one = System.currentTimeMillis()
//    deltaTable.toDF
//      .where("yyyy = 2022 AND mm = 2 AND dd > 3")
//      .show()
//
//    println(s"Took ${System.currentTimeMillis()-start_one}ms")

    //deltaTable.optimize()
    //  .executeZOrderBy("yyyy","mm","dd")

//    val start_two = System.currentTimeMillis()
//    deltaTable.toDF
//      .where("date > date('2022-02-03')")
//      .show()
//
//    println(s"Took ${System.currentTimeMillis() - start_two}ms")

    //val df = spark.read.delta(pathToTable)
    //df.show()

    //deltaTable.optimize()
    //  .executeZOrderBy("date", "data")
      //.executeCompaction()

    // If you have a large amount of data and only want to optimize a subset of it, you can specify an optional partition predicate using `where`
    //deltaTable.optimize().where("date='2021-11-18'").executeCompaction()

    deltaTable.vacuum(0.0)
  }

  test("Tests Delta Optimize Batch") {
    val path = "src/test/tmp/delta/letters_optimize"
    val sqlCtx = sqlContext

    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(someSchema)
    )

    //Create table
    val emptyRDD = spark.sparkContext.emptyRDD[Row]
    val emptyDF = spark.createDataFrame(emptyRDD, inputDF.schema)
    emptyDF
      .write
      .format("delta")
      .mode(SaveMode.Append)
      .save(path)

    val firstWriter = new DeltaWriter(
      path,
      WriteMode.Overwrite,
      Option("date"),
      Option("name"),
      "default",
      "",
      Seq.empty[String],
      0)(region, spark)


    firstWriter.write(inputDF, EngineMode.Batch)

    val deltaTable = DeltaTable.forPath(path)

    //Get last operation
    val lastChange = deltaTable.history(1)
    val operation = lastChange.head().getAs[String]("operation")

    assert(operation == "OPTIMIZE")

  }

}