package com.metabolic.data.core.services.catalogue

import com.metabolic.data.core.services.util.ConfigUtilsService
import com.metabolic.data.mapper.domain.Config
import com.metabolic.data.mapper.domain.io._
import org.apache.logging.log4j.scala.Logging

import java.math.BigInteger
import java.security.MessageDigest
import scala.collection.mutable
import com.metabolic.data.core.services.catalogue.ColumnLineage
import com.metabolic.data.mapper.domain.ops.{Mapping, SQLMapping}
import org.apache.spark.sql.catalog.Table
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class AtlanService(token: String, spark: SparkSession) extends Logging {

  val versionRegex = """version=(\d)+/""".r

  def setLineage(mapping: Config, table: Boolean, column: Boolean): Unit = {
    table match {
        case true => setTableLineage(mapping)
        case _ => ""
    }
    column match {
      case true => setColumnLineage(mapping)
      case _ => ""
    }
  }
  def setTableLineage(mapping: Config): String = {
    val body = generateBodyJson(mapping)
    logger.info(s"Atlan Json Body ${body}")
    HttpRequestHandler.sendHttpPostRequest("https://factorial.atlan.com/api/meta/entity/bulk#createProcesses", body, token)
  }

  def setColumnLineage(mapping: Config)= {
    val outputTable = getOutputTableName(mapping)
    val dbName = mapping.environment.dbName

    mapping match {
      case sqlmapping: SQLMapping => {
        val columnLineage = new ColumnLineage().traversePlan(spark.sql(sqlmapping.sqlContents).queryExecution.logical)
        // Get the list of registered sources (DataFrames)
        val registeredSources = spark.catalog.listTables().filter(_.isTemporary)
        val output: DataFrame = spark.table("output")

          val result = output.schema.foldLeft(()) { (acc, col) =>
            val inputSources: Map[String, (String, String)] =
              columnLineage.contains(col.name) match {
                case true => {
                  val inputs = columnLineage.get(col.name).toString
                  val parts = inputs.split(",")
                  parts.map { part =>
                    val (tableName, inputColumn) = findInputLineage(part, registeredSources, columnLineage)
                    col.name -> (inputColumn, tableName)
                  }.toMap
                }
                case _ => Map(col.name -> ("", ""))
          }
          val body = generateBodyJsonForColumns(inputSources, outputTable, dbName)
          logger.info(s"Atlan Json Body ${body}")
          HttpRequestHandler.sendHttpPostRequest("https://factorial.atlan.com/api/meta/entity/bulk#createColumnProcesses", body, token)
          acc
        }
      }
    }
  }

  def findInputLineage(column: String, sources: Dataset[Table], columnLineage: mutable.Map[String, mutable.Set[String]] ): (String, String) = {

    def findHelper(column: String, visited: Set[String]): (String, String) = {
      columnLineage.contains(column) match {
        case true => {
          val input = columnLineage.get(column).toString
          val matchedView = sources.filter(view =>
            spark.table(view.name).schema.fieldNames.contains(input)
          )
          if (matchedView.isEmpty || visited.contains(input)) {
            findHelper(input, visited + column)
          } else {
            (matchedView.head.name, input)
          }
          //          sources.foreach(view => spark.table(view.name).schema.contains(input) match {
          //            case true => (view.name, input)
          //            case false => findInputLineage(input, sources, columnLineage)
          //          })
        }
        case false => ("", "")
      }
    }
      findHelper(column, Set())
  }


  def generateBodyJson(mapping: Config): String = {
    val inputTables = getSourceTableNameList(mapping)
    val outputTable = getOutputTableName(mapping)
    val baseUrl = "default/athena/1659962653/AwsDataCatalog/"
    val dbName = mapping.environment.dbName
    val name = inputTables.mkString(",") + " -> " + s"${dbName}/${outputTable}"
    val qualifiedName = baseUrl + md5Hash(name)
    val body: String =
      s"""
         |{
         |  "entities": [
         |    {
         |      "typeName": "Process",
         |      "attributes": {
         |        "name": "${name}",
         |        "qualifiedName": "${qualifiedName}",
         |        "connectorName": "athena",
         |        "connectionName": "athena",
         |        "connectionQualifiedName": "${baseUrl.dropRight(1)}"
         |      },
         |      "relationshipAttributes": {
         |      "outputs": [
         |          {
         |            "typeName": "Table",
         |            "uniqueAttributes": {
         |              "qualifiedName": "${baseUrl}${dbName}/${outputTable}"
         |            }
         |          }
         |        ],
         |        "inputs": [
         |""".stripMargin


    val bodyWithSourcesNames = inputTables.foldLeft(body) { (body: String, sourceTableName: String) =>

      body +
        s"""|          {
            |            "typeName": "Table",
            |            "uniqueAttributes": {
            |              "qualifiedName": "${baseUrl}${sourceTableName}"
            |            }
            |          },""".stripMargin

    }

    bodyWithSourcesNames
      .dropRight(1) +
      """
        |        ]
        |      }
        |    }
        |  ]
        |}""".stripMargin

  }


  def generateBodyJsonForColumns(Columns: Map[String, (String, String)], outputTable: String, dbName: String): String = {
    val baseUrl = "default/athena/1659962653/AwsDataCatalog/"
    val outputColumn = Columns.keys.headOption
    val formattedName: String = Columns.values.map { case (inputColumn, tableName) =>
      s"default/athena/$tableName/$inputColumn"
    }.mkString(",") + " -> " + s"default/athena/${outputTable}/${outputColumn}"
    val qualifiedName = baseUrl + md5Hash(formattedName)
    val body: String =
      s"""
         |{
         |  "entities": [
         |    {
         |      "typeName": "ColumnProcess",
         |      "attributes": {
         |        "name": "${formattedName}",
         |        "qualifiedName": "${qualifiedName}",
         |        "connectorName": "athena",
         |        "connectionName": "athena",
         |        "connectionQualifiedName": "${baseUrl.dropRight(1)}"
         |      },
         |      "relationshipAttributes": {
         |      "outputs": [
         |          {
         |            "typeName": "Column",
         |            "uniqueAttributes": {
         |              "qualifiedName": "${baseUrl}${dbName}/${outputTable}/${outputColumn}"
         |            }
         |          }
         |        ],
         |        "inputs": [
         |""".stripMargin


    val bodyWithSourcesNames = Columns.values.foldLeft(body) { (body, col) =>
      val (sourceTableName, sourceInputColumn) = col
      body +
        s"""|          {
            |            "typeName": "Table",
            |            "uniqueAttributes": {
            |              "qualifiedName": "${baseUrl}${dbName}/${sourceTableName}/${sourceInputColumn}"
            |            }
            |          },""".stripMargin

    }

    bodyWithSourcesNames
      .dropRight(1) +
      """
        |        ]
        |      }
        |    }
        |  ]
        |}""".stripMargin

  }

  def md5Hash(pwd: String): String = {
    val digest: Array[Byte] = MessageDigest.getInstance("MD5").digest(pwd.getBytes)
    val bigInt = new BigInteger(1, digest).toString(16).trim
    "%1$32s".format(bigInt).replace(' ', '0')
  }

  private def getSourceTableNameList(config: Config): mutable.MutableList[String] = {
    val options = config.environment
    val prefix_namespaces = options.namespaces
    val infix_namespaces = options.infix_namespaces
    val tables = mutable.MutableList[String]()
    config.sources.foreach { source =>
      tables += getSourceTableName(source, prefix_namespaces,infix_namespaces, options.dbName)
    }
    tables.filter(p => p != "")
  }

  private def getSourceTableName(source: Source,  prefix_namespaces:Seq[String],infix_namespaces:Seq[String], dbName: String): String = {


    source match {

      case streamSource: StreamSource => ""

      case fileSource: FileSource => {

        val s3Path = versionRegex.replaceAllIn(fileSource.inputPath, "")
        val prefix = ConfigUtilsService.getTablePrefix(prefix_namespaces, s3Path)
        val infix = ConfigUtilsService.getTableInfix(infix_namespaces, s3Path)
        val tableName = ConfigUtilsService.getTableNameFileSink(s3Path)
        dbName + "/" + prefix + infix + tableName
      }
      case meta: MetastoreSource => meta.name
    }
  }

  private def getOutputTableName(mapping: Config): String = {
    //Extract table name from s3 path
    val s3Path = versionRegex.replaceAllIn(mapping.sink.asInstanceOf[FileSink].path, "")
    ConfigUtilsService.getTablePrefix(mapping.environment.namespaces, s3Path) +
      ConfigUtilsService.getTableInfix(mapping.environment.infix_namespaces, s3Path) +
      ConfigUtilsService.getTableName(mapping)
  }

}