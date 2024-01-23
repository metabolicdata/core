import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.mapper.app.MetabolicApp
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import io.delta.tables._
import org.apache.spark.SparkConf

class DeltaOptimizeTest
  extends AnyFunSuite
    with DataFrameSuiteBase
    with SharedSparkContext {


  override def conf: SparkConf = super.conf
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .set("spark.databricks.delta.retentionDurationCheck.enabled","false")

  test("Delta Optimze") {

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

    //deltaTable.vacuum(0.0)
  }
}