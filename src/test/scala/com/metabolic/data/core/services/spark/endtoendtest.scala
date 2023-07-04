import MapperEntrypoint.argMap
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import com.metabolic.data.mapper.app.MetabolicApp
import org.apache.spark.sql.SparkSession
import org.scalatest.Ignore
import org.scalatest.funsuite.AnyFunSuite


class EndToEndTest
  extends AnyFunSuite
    with DataFrameSuiteBase
    with SharedSparkContext {
  test("End2End") {


    val input = Array(
      "--dp.region",      "eu-central-1",
      "--dp.historical",  "false",
      "--dp.stream",      "true",
      "--dp.iamrole",     "dev-feature_production_upgrade-MetabolicRole",
      "--dp.crawl",       "true",
      "--dp.envPrefix",   "dev/feature_production_upgrade",
      "--dp.environment", "production",
      "--dp.database",    "dev_feature_production_upgrade_data_lake",
      "--dp.checkpointLocation", ".",
      "--configFile",     "src/test/resources/example.conf"
    )

    val parsedArgs = MapperEntrypoint.argumentsAsMap(input)

    new MetabolicApp(SparkSession.builder())
      .run(parsedArgs("configFile"), parsedArgs)
  }

}
