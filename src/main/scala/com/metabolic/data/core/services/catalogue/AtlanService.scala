package com.metabolic.data.core.services.catalogue

import com.metabolic.data.core.services.util.ConfigUtilsService
import com.metabolic.data.mapper.domain.Config
import com.metabolic.data.mapper.domain.io._
import com.metabolic.data.mapper.domain.ops.SQLMapping
import org.apache.hadoop.shaded.com.google.gson.JsonParseException
import org.apache.logging.log4j.scala.Logging
import play.api.libs.json._

import java.math.BigInteger
import java.security.MessageDigest
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable

class AtlanService(token: String, baseUrlDataLake: String, baseUrlConfluent: String) extends Logging {

  val versionRegex = """version=(\d)+/""".r

  def setLineage(mapping: Config): String = {
    val body = generateBodyJson(mapping)
    logger.info(s"Atlan Json Body ${body}")
    HttpRequestHandler.sendHttpPostRequest("https://factorial.atlan.com/api/meta/entity/bulk#createProcesses", body, token)
  }

  def setMetadata(mapping: Config): String = {
    val qualifiedName = getQualifiedNameOutput(mapping)
    val guid = getGUI(qualifiedName).stripPrefix("\"").stripSuffix("\"")
    guid match {
      case "" => ""
      case _ => {
        val body: String = generateMetadaBody(mapping)
        logger.info(s"Atlan Metadata Json Body ${body}")
        HttpRequestHandler.sendHttpPostRequest(s"https://factorial.atlan.com/api/meta/entity/guid/$guid/businessmetadata/displayName?isOverwrite=false", body, token)
      }
    }
  }

  def generateMetadaBody(mapping: Config): String = {
    val last_synced = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val mode = mapping.environment.mode
    val sql = mapping.mappings.head match {
      case sqlmapping: SQLMapping => {
        sqlmapping.sqlContents
      }
      case _ => ""
    }
    val body =
      s"""
         |{
         |  "Data Quality": {
         |    "last_synced_at" : "${last_synced}",
         |    "engine_type":"${mode.toString}",
         |    "sql_mapping":"$sql"
         |  }
         |}
         |""".stripMargin
    body
  }

  def setDescription(mapping: Config): String = {
    val outputTable = getOutputTableName(mapping)
    val qualifiedName = getQualifiedNameOutput(mapping)
    val typeName = getTypeName(mapping)
    val body = generateDescriptionBodyJson(outputTable, qualifiedName, typeName)
    logger.info(s"Atlan Description Json Body ${body}")
    HttpRequestHandler.sendHttpPostRequest(s"https://factorial.atlan.com/api/meta/entity/bulk#changeDescriptione", body, token)
  }

  private def generateDescriptionBodyJson(outputTable: String, qualifiedName: String, typeName: String): String = {
    s"""
       |{
       |  "entities": [
       |    {
       |      "typeName": "$typeName",
       |      "attributes": {
       |        "name": "$outputTable",
       |        "qualifiedName": "$qualifiedName",
       |        "description": ""
       |      }
       |    }
       |  ]
       |}
       |""".stripMargin
  }

  def generateBodyJson(mapping: Config): String = {
    val inputTables = getSourceTableNameList(mapping)
    val outputTable = getOutputTableName(mapping)
    val outputType = getTypeName(mapping)
    val name = inputTables.mkString(",") + " -> " + outputTable
    val qualifiedName = getQualifiedNameProcess(mapping, name)
    val connectorName = getConnectorNameProcess(mapping)
    val connectionName = getConnectionNameProcess(mapping)
    val connectionQualifiedName = getConnectionQualifiedNameProcess(mapping)
    val qualifiedNameOutput = getQualifiedNameOutput(mapping)
    val qualifiedNameInputs = getQualifiedNameInputs(mapping)

    val inputsJson = qualifiedNameInputs.map { case (sourceType, qualifiedName) =>
      s"""
         |          {
         |            "typeName": "${sourceType}",
         |            "uniqueAttributes": {
         |              "qualifiedName": "${qualifiedName}"
         |            }
         |          }"""
    }.mkString(",").stripMargin

    s"""
       |{
       |  "entities": [
       |    {
       |      "typeName": "Process",
       |      "attributes": {
       |        "name": "${name}",
       |        "qualifiedName": "${qualifiedName}",
       |        "connectorName": "${connectorName}",
       |        "connectionName": "${connectionName}",
       |        "connectionQualifiedName": "${connectionQualifiedName}"
       |      },
       |      "relationshipAttributes": {
       |        "outputs": [
       |          {
       |            "typeName": "${outputType}",
       |            "uniqueAttributes": {
       |              "qualifiedName": "${qualifiedNameOutput}"
       |            }
       |          }
       |        ],
       |        "inputs": [
       |${inputsJson}
       |        ]
       |      }
       |    }
       |  ]
       |}""".stripMargin

  }

  private def md5Hash(pwd: String): String = {
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
      tables += getSourceTableName(source, prefix_namespaces, infix_namespaces, options.dbName)
    }
    tables.filter(p => p != "")
  }

  private def getQualifiedNameInputs(config: Config): Seq[(String, String)] = {
    val options = config.environment
    val prefix_namespaces = options.namespaces
    val infix_namespaces = options.infix_namespaces

    config.sources.flatMap { source =>
      val qualifiedName = getQualifiedNameInput(source, prefix_namespaces, infix_namespaces, options.dbName)
      if (qualifiedName.nonEmpty) Some((getTypeName(source), qualifiedName)) else None
    }
  }

  private def getSourceTableName(source: Source, prefix_namespaces: Seq[String], infix_namespaces: Seq[String], dbName: String): String = {

    source match {

      case streamSource: StreamSource => streamSource.topic

      case fileSource: FileSource => {
        val s3Path = versionRegex.replaceAllIn(fileSource.inputPath, "")
        val prefix = ConfigUtilsService.getTablePrefix(prefix_namespaces, s3Path)
        val infix = ConfigUtilsService.getTableInfix(infix_namespaces, s3Path)
        val tableName = ConfigUtilsService.getTableNameFileSink(s3Path)
        dbName + "/" + prefix + infix + tableName
      }

      case meta: TableSource => meta.fqn.replace(".", "/")
    }
  }

  private def getQualifiedNameInput(source: Source, prefix_namespaces: Seq[String], infix_namespaces: Seq[String], dbName: String): String = {

    source match {

      case streamSource: StreamSource => baseUrlConfluent + streamSource.topic

      case fileSource: FileSource => {
        val s3Path = versionRegex.replaceAllIn(fileSource.inputPath, "")
        val prefix = ConfigUtilsService.getTablePrefix(prefix_namespaces, s3Path)
        val infix = ConfigUtilsService.getTableInfix(infix_namespaces, s3Path)
        val tableName = ConfigUtilsService.getTableNameFileSink(s3Path)
        baseUrlDataLake + dbName + "/" + prefix + infix + tableName
      }

      case meta: TableSource => baseUrlDataLake + meta.fqn.replace(".", "/")
    }
  }

  private def getQualifiedNameProcess(mapping: Config, name: String): String = {

    mapping.sink match {
      case _: StreamSink =>
        baseUrlConfluent + md5Hash(name)
      case _: FileSink =>
        baseUrlDataLake + md5Hash(name)
    }

  }

  private def getConnectorNameProcess(mapping: Config): String = mapping.sink match {
    case _: StreamSink => "confluent-kafka"
    case _: FileSink => "athena"
  }

  private def getConnectionNameProcess(mapping: Config): String = mapping.sink match {
    case _: StreamSink => "production"
    case _: FileSink => "athena"
  }

  private def getConnectionQualifiedNameProcess(mapping: Config): String = {
    val baseUrl = if (mapping.sink.isInstanceOf[StreamSink]) baseUrlConfluent else baseUrlDataLake
    baseUrl.dropRight(1)
  }

  private def getQualifiedNameOutput(mapping: Config): String = {
    val baseUrl = if (mapping.sink.isInstanceOf[StreamSink]) baseUrlConfluent else baseUrlDataLake
    s"$baseUrl${getOutputTableName(mapping)}"
  }

  private def getOutputTableName(mapping: Config): String = {
    //Extract table name from s3 path
    mapping.sink match {
      case fileSink: FileSink => {
        val s3Path = versionRegex.replaceAllIn(fileSink.path, "")
        val prefix = ConfigUtilsService.getTablePrefix(mapping.environment.namespaces, s3Path)
        val infix = ConfigUtilsService.getTableInfix(mapping.environment.infix_namespaces, s3Path)
        val tableName = ConfigUtilsService.getTableName(mapping)
        val dbName = mapping.environment.dbName
        dbName + "/" + prefix + infix + tableName
      }
      case streamSink: StreamSink => {
        ConfigUtilsService.getTableName(mapping)
      }
      case tableSink: TableSink => {
        tableSink.catalog.replace(".", "/")
      }
    }

  }

  private def getTypeName(mapping: Config): String = {
    // Extract typeName from sink type
    mapping.sink match {
      case _: StreamSink =>
        "KafkaTopic"
      case _: FileSink =>
        "Table"
    }
  }

  private def getTypeName(source: Source): String = {
    // Extract typeName from sink type
    source match {
      case _: StreamSource =>
        "KafkaTopic"
      case _: FileSource =>
        "Table"
      case _: TableSource =>
        "Table"
    }
  }

  def getGUI(qualifiedName: String): String = {
    try {
      val response = HttpRequestHandler.sendHttpGetRequest(s"https://factorial.atlan.com/api/meta/entity/uniqueAttribute/type/Table?attr:qualifiedName=${qualifiedName}", token)
      isValidJson(response) match {
        case true => {
          val json = Json.parse(response)
          json.\("entity").get("guid").toString()
        }
        case false => {
          logger.info(s"can not find obj ${qualifiedName}")
          ""
        }
      }
    }
    catch {
      case e: Exception =>
        println(s"An error occurred: ${e.getMessage}")
        ""
    }
  }

  private def isValidJson(jsonString: String): Boolean = {
    try {
      Json.parse(jsonString)
      true
    } catch {
      case _: JsonParseException => false
    }
  }
}
