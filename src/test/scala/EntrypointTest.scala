import org.scalatest.funsuite.AnyFunSuite

class EntrypointTest extends AnyFunSuite {

  test("Entity Mapper Entrypoint args") {
    val input = Array("--dp.mappings_bucket","s3://dummy-client/production/mappings",
                     "--dp.crawl","true",
                     "--configFile","s3://dummy-client/production/mappings/test/test.conf")

    val expected = Map("dp.mappings_bucket" -> "s3://dummy-client/production/mappings",
                       "dp.crawl" -> "true",
                       "configFile"-> "s3://dummy-client/production/mappings/test/test.conf")

    val parsedArgs = MapperEntrypoint.argumentsAsMap(input)

    assert(parsedArgs == expected)
  }



}
