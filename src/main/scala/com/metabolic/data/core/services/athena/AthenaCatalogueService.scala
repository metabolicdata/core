package com.metabolic.data.core.services.athena

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.athena._
import com.amazonaws.services.athena.model.{GetQueryResultsRequest, QueryExecutionContext, StartQueryExecutionRequest}
import org.apache.logging.log4j.scala.Logging


class AthenaCatalogueService(implicit val region: Regions) extends Logging {

  val awsCredentials = new DefaultAWSCredentialsProviderChain()

  val athenaClient: AmazonAthena = AmazonAthenaClientBuilder.standard()
    .withCredentials(awsCredentials)
    .withRegion(region)
    .build()

  private def createTableStatement(dbName: String, tableName: String, s3_location: String) = {

    s"CREATE EXTERNAL TABLE IF NOT EXISTS " +
      s"`$dbName`.`$tableName`" +
      s" LOCATION '$s3_location'" +
      "TBLPROPERTIES ('table_type' = 'DELTA')"

  }

  private def dropTableStatement(dbName: String, tableName: String) = {

    s"DROP TABLE IF EXISTS " +
      s"`$dbName.\"$tableName\"`"
  }

  def dropView(dbName: String, viewName: String) = {

    val statement = s"DROP VIEW IF EXISTS " +
      s"$dbName.$viewName"

    dropTable(dbName, viewName, statement)

  }

  def createDeltaTable(dbName:String, tableName:String, location: String, recreate: Boolean = false) = {

    if(recreate) {
      val delete_statement = dropTableStatement(dbName, tableName)
      dropTable(dbName, tableName, delete_statement)
    }

    val statement = createTableStatement(dbName, tableName, location)
    logger.info(s"Create table statement for ${dbName}.${tableName} is ${statement}")
    val queryExecutionContext = new QueryExecutionContext().withDatabase(dbName)
    //val resultConfiguration = new ResultConfiguration().withOutputLocation(path)

    val startQueryExecutionRequest = new StartQueryExecutionRequest()
      .withQueryString(statement)
      .withQueryExecutionContext(queryExecutionContext)
      //.withResultConfiguration(resultConfiguration)
    val queryExecutionId = athenaClient.startQueryExecution(startQueryExecutionRequest).getQueryExecutionId
    logger.info(s"Table ${dbName}.${tableName} has been created")
    val getQueryResultsRequest = new GetQueryResultsRequest().withQueryExecutionId(queryExecutionId)

  }

  private def dropTable(dbName: String, tableName: String, statement: String) = {

    logger.info(s"Drop table statement for ${dbName}.${tableName} is ${statement}")
    val queryExecutionContext = new QueryExecutionContext().withDatabase(dbName)
    //val resultConfiguration = new ResultConfiguration().withOutputLocation(path)

    val startQueryExecutionRequest = new StartQueryExecutionRequest()
      .withQueryString(statement)
      .withQueryExecutionContext(queryExecutionContext)
    //.withResultConfiguration(resultConfiguration)
    val queryExecutionId = athenaClient.startQueryExecution(startQueryExecutionRequest).getQueryExecutionId
    val getQueryResultsRequest = new GetQueryResultsRequest().withQueryExecutionId(queryExecutionId)
    logger.info(s"Query result is: ${getQueryResultsRequest}")

  }



    //athenaClient.

  /*

  import com.amazonaws.services.athena.model.QueryExecutionContext
  import com.amazonaws.services.athena.model.ResultConfiguration
  import com.amazonaws.services.athena.model.StartQueryExecutionRequest

  def submitAthenaQuery(athenaClient: AmazonAthena, database: String): String = {
    try { // The QueryExecutionContext allows us to set the database
      val qec = new QueryExecutionContext().setDatabase(database)

      // The result configuration specifies where the results of the query should go
      val resultConfiguration = ResultConfiguration.builder.outputLocation(ExampleConstants.ATHENA_OUTPUT_BUCKET).build
      val startQueryExecutionRequest = StartQueryExecutionRequest.builder.queryString(ExampleConstants.ATHENA_SAMPLE_QUERY).queryExecutionContext(queryExecutionContext).resultConfiguration(resultConfiguration).build
      val startQueryExecutionResponse = athenaClient.startQueryExecution(startQueryExecutionRequest)
      return startQueryExecutionResponse.queryExecutionId
    } catch {
      case e: Nothing =>
        e.printStackTrace
        System.exit(1)
    }
    ""
  }


  import com.amazonaws.services.athena.model.ColumnInfo
  import com.amazonaws.services.athena.model.Datum
  import com.amazonaws.services.athena.model.GetQueryExecutionRequest
  import com.amazonaws.services.athena.model.GetQueryResultsRequest
  import com.amazonaws.services.athena.model.QueryExecutionState

  // Wait for an Amazon Athena query to complete, fail or to be cancelled
  @throws[InterruptedException]
  def waitForQueryToComplete(athenaClient: Nothing, queryExecutionId: String): Unit = {
    val getQueryExecutionRequest = GetQueryExecutionRequest.builder.queryExecutionId(queryExecutionId).build
    var getQueryExecutionResponse = null
    var isQueryStillRunning = true
    while ( {
      isQueryStillRunning
    }) {
      getQueryExecutionResponse = athenaClient.getQueryExecution(getQueryExecutionRequest)
      val queryState = getQueryExecutionResponse.queryExecution.status.state.toString
      if (queryState == QueryExecutionState.FAILED.toString) throw new RuntimeException("The Amazon Athena query failed to run with error message: " + getQueryExecutionResponse.queryExecution.status.stateChangeReason)
      else if (queryState == QueryExecutionState.CANCELLED.toString) throw new RuntimeException("The Amazon Athena query was cancelled.")
      else if (queryState == QueryExecutionState.SUCCEEDED.toString) isQueryStillRunning = false
      else { // Sleep an amount of time before retrying again
        Thread.sleep(ExampleConstants.SLEEP_AMOUNT_IN_MS)
      }
      System.out.println("The current status is: " + queryState)
    }
  }

  // This code retrieves the results of a query
  def processResultRows(athenaClient: Nothing, queryExecutionId: String): Unit = {
    try { // Max Results can be set but if its not set,
      // it will choose the maximum page size
      val getQueryResultsRequest = GetQueryResultsRequest.builder.queryExecutionId(queryExecutionId).build
      val getQueryResultsResults = athenaClient.getQueryResultsPaginator(getQueryResultsRequest)
      import scala.collection.JavaConversions._
      for (result <- getQueryResultsResults) {
        val columnInfoList = result.resultSet.resultSetMetadata.columnInfo
        val results = result.resultSet.rows
        processRow(results, columnInfoList)
      }
    } catch {
      case e: Nothing =>
        e.printStackTrace
        System.exit(1)
    }
  }

  private def processRow(row: Nothing, columnInfoList: Nothing): Unit = {
    import scala.collection.JavaConversions._
    for (myRow <- row) {
      val allData = myRow.data
      import scala.collection.JavaConversions._
      for (data <- allData) {
        System.out.println("The value of the column is " + data.varCharValue)
      }
    }
  }

  */


}
