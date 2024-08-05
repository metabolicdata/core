package com.metabolic.data.core.services.athena

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.athena._
import com.amazonaws.services.athena.model.{GetQueryExecutionRequest, GetQueryExecutionResult, QueryExecutionContext, StartQueryExecutionRequest}
import org.apache.kafka.common.utils.Utils.sleep
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
      s"""`$dbName.$tableName`"""
  }

  private def dropViewStatement(dbName: String, viewName: String): String = {
    s"DROP VIEW IF EXISTS " +
      s"$dbName.$viewName"
  }

  def dropView(dbName: String, viewName: String) = {
    val statement = dropViewStatement(dbName, viewName)
    athenaQueryExecution(dbName, statement)
  }

  def getQueryExecutionState(queryExecutionId: String): String = {
    val getQueryExecutionRequest = new GetQueryExecutionRequest().withQueryExecutionId(queryExecutionId)
    val getQueryExecutionResult: GetQueryExecutionResult = athenaClient.getQueryExecution(getQueryExecutionRequest)
    getQueryExecutionResult.getQueryExecution.getStatus.getState
  }

  def athenaQueryExecution(dbName: String, statement: String): String = {
    val queryExecutionContext = new QueryExecutionContext().withDatabase(dbName)
    val startQueryExecutionRequest = new StartQueryExecutionRequest()
      .withQueryString(statement)
      .withQueryExecutionContext(queryExecutionContext)
    val queryExecutionId = athenaClient.startQueryExecution(startQueryExecutionRequest).getQueryExecutionId

    queryExecutionId
  }

  def getQueryStatus(queryExecutionId: String): String = {
    var queryState = "RUNNING"

    while (queryState != "SUCCEEDED" && queryState != "FAILED") {
      sleep(1000)
      queryState = getQueryExecutionState(queryExecutionId)
    }
    queryState
  }

  def checkSchemaChange(dbName: String, tableName: String): Boolean = {
    val statement = s"SELECT 1 FROM " +
      s"$dbName.$tableName"
    val queryExecutionId = athenaQueryExecution(dbName, statement)

    logger.info(s"Checking if schema has changed... Waiting for query $queryExecutionId to complete...")
    val queryState = getQueryStatus(queryExecutionId)
    queryState == "FAILED"
  }

  def createDeltaTable(dbName:String, tableName:String, location: String) = {

    val recreate = checkSchemaChange(dbName, tableName)

    if(recreate) {
      logger.info(s"Schema has changed for table $tableName")
      val delete_statement = dropTableStatement(dbName, tableName)
      logger.info(s"Drop table statement for ${dbName}.${tableName} is ${delete_statement}")

      val queryExecutionId = athenaQueryExecution(dbName, delete_statement)

      // Wait for table deletion
      logger.info(s"Waiting for query $queryExecutionId to complete...")
      getQueryStatus(queryExecutionId)
    }

    val statement = createTableStatement(dbName, tableName, location)
    logger.info(s"Create table statement for ${dbName}.${tableName} is ${statement}")
    val queryExecutionId = athenaQueryExecution(dbName, statement)

    logger.info(s"Table ${dbName}.${tableName} has been created")
  }

}
