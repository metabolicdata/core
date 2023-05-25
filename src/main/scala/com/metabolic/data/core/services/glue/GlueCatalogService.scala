package com.metabolic.data.core.services.glue

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.glue._
import com.amazonaws.services.glue.model._
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.collection.JavaConverters._


class GlueCatalogService(implicit val region: Regions) extends Logging {

  val awsCredentials = new DefaultAWSCredentialsProviderChain()

  val glueClient = AWSGlueClientBuilder.standard()
    .withCredentials(awsCredentials)
    .withRegion(region)
    .build


  def getTableSchema(dbName: String, tableName: String): StructType = {

    val tableVersionsRequest = new GetTableVersionsRequest()
      .withDatabaseName(dbName)
      .withTableName(tableName)

    val results = glueClient.getTableVersions(tableVersionsRequest)
    val versions = results.getTableVersions
    val tableColumns = versions.get(0).getTable.getStorageDescriptor.getColumns()

    // Convert A List Of Column Objects To Struct
    val schema = tableColumns.asScala.map(column => {
      StructField(column.getName, toSparkType(column.getType))
    }).toList

    StructType(schema)
  }

  def toSparkType(inputType: String): DataType =
    CatalystSqlParser.parseDataType(inputType)


  def createTable(dbName: String, tableName: String, dataFrame: DataFrame, s3_location: String): Unit = {

    val catalog_id = "catalog_id"

    val columns = dataFrame.schema.fields.map { field =>
      new Column().withName(field.name).withType(field.dataType.simpleString)
    }

    val tableInput = new TableInput()
      .withName(tableName)
      .withStorageDescriptor(new StorageDescriptor()
        .withLocation(s3_location)
        .withInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
        .withOutputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat")
        .withSerdeInfo(new SerDeInfo()
          .withSerializationLibrary("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"))
        .withColumns(columns: _*))

    val getTableRequest = new GetTableRequest()
      .withCatalogId(catalog_id)
      .withDatabaseName(dbName)
      .withName(tableName)

    try {
      glueClient.getTable(getTableRequest).getTable
      // Table already exists, update it
      logger.info(s"Table $tableName already exists. Update it.")
      new UpdateTableRequest()
        .withCatalogId(catalog_id)
        .withDatabaseName(dbName)
        .withTableInput(tableInput)
    } catch {
      case e: com.amazonaws.services.glue.model.EntityNotFoundException =>
        // Create table
        val createTableRequest = new CreateTableRequest()
          .withDatabaseName(dbName)
          .withTableInput(tableInput)
          .withCatalogId(catalog_id)
        glueClient.createTable(createTableRequest)
        logger.info(s"Table $tableName created.")
    }

  }

  def checkDatabase(dbName: String, s3_location: String): Unit = {

    val getDatabaseRequest = new GetDatabaseRequest()
      .withName(dbName.toLowerCase)

    val databaseInput = new DatabaseInput()
      .withName(dbName.toLowerCase)
      .withLocationUri(s3_location)

    val updateDatabaseRequest = new UpdateDatabaseRequest()
      .withName(dbName.toLowerCase)
      .withDatabaseInput(databaseInput)

    try {
      val database: Database = glueClient.getDatabase(getDatabaseRequest).getDatabase
      if (database.getLocationUri == null || database.getLocationUri.isEmpty) {
        logger.info(s"Database $dbName updated.")
        //Set database for table
        glueClient.updateDatabase(updateDatabaseRequest)
      }
    } catch {
      case e: com.amazonaws.services.glue.model.EntityNotFoundException =>
        // Create database
        val createDataseRequest = new CreateDatabaseRequest()
          .withDatabaseInput(databaseInput)
        glueClient.createDatabase(createDataseRequest)
        logger.info(s"Database $dbName created.")
    }
  }


}
