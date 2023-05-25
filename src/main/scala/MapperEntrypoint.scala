
import com.metabolic.data.mapper.app.MetabolicApp
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession


object MapperEntrypoint extends App with Logging {

  /** Assuming a args array like ["--optA", "1", "--optB", "2"] passed from main
   *  this function will return a map with shape ("optA" -> "1", "optB" -> "2")
   */
  def argumentsAsMap(args: Array[String]): Map[String, String] = {
    val arrayToMap = (total: Map[String, String], element: Array[String]) => total + (element(0) -> element(1))
    val newMap = Map[String,String]()

    args.mkString(" ")
      .split("--")
      .map(_.split(" "))
      .filter(_.size == 2)
      .foldLeft(newMap)(arrayToMap)
  }

  logger.info(s"Incoming args ${args.mkString(" ")}")

  private val argMap = argumentsAsMap(args)

  logger.info(s"Running Metabolic Mapper ${argMap("configFile")} with params: $argMap  }")

  val sparkBuilder = SparkSession.builder()
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "LEGACY")
    .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")
    .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY")
    .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")

  new MetabolicApp(sparkBuilder)
    .run(argMap("configFile"), argMap)

}
