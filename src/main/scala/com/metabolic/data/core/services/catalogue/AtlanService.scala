package com.metabolic.data.core.services.catalogue

import com.metabolic.data.core.services.util.ConfigUtilsService
import com.metabolic.data.mapper.domain.Config
import com.metabolic.data.mapper.domain.io.EngineMode.EngineMode
import com.metabolic.data.mapper.domain.io._
import com.metabolic.data.mapper.domain.ops.SQLMapping
import org.apache.hadoop.shaded.com.google.gson.JsonParseException
import org.apache.logging.log4j.scala.Logging

import java.math.BigInteger
import java.security.MessageDigest
import scala.collection.mutable
import play.api.libs.json._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


class AtlanService(token: String) extends Logging {

  val versionRegex = """version=(\d)+/""".r
  private val baseUrl = "default/athena/1659962653/AwsDataCatalog/"

  def setLineage(mapping: Config): String = {
    val body = generateBodyJson(mapping)
    logger.info(s"Atlan Json Body ${body}")
    HttpRequestHandler.sendHttpPostRequest("https://factorial.atlan.com/api/meta/entity/bulk#createProcesses", body, token)
  }

  def setMetadata(mapping: Config): String = {
    val outputTable = getOutputTableName(mapping)
    val dbName = mapping.environment.dbName
    val qualifiedName = s"${baseUrl}${dbName}/${outputTable}"
    val guid = getGUI(qualifiedName).stripPrefix("\"").stripSuffix("\"")
    guid match{
      case "" => ""
      case _ => {
        val last_synced = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        val mode = mapping.environment.mode
        val body =
          s"""
             |{
             |  "Data Quality": {
             |    "last_synced_at" : "${last_synced}",
             |    "engine_type":"${mode.toString}"
             |  }
             |}
             |""".stripMargin
        logger.info(s"Atlan Metadata Json Body ${body}")
        HttpRequestHandler.sendHttpPostRequest(s"https://factorial.atlan.com/api/meta/entity/guid/$guid/businessmetadata/displayName?isOverwrite=false", body, token)
      }
    }
  }

  def setDescription(mapping: Config): String = {
    val outputTable = getOutputTableName(mapping)
    val dbName = mapping.environment.dbName
    val qualifiedName = s"${baseUrl}${dbName}/${outputTable}"
    val body = generateDescriptionBodyJson(mapping, outputTable, qualifiedName)
    logger.info(s"Atlan Description Json Body ${body}")
    HttpRequestHandler.sendHttpPostRequest(s"https://factorial.atlan.com/api/meta/entity/bulk#changeDescriptione", body, token)
  }

  def generateDescriptionBodyJson(mapping: Config, outputTable: String, qualifiedName: String): String = {
    val sql = mapping.mappings.head match {
      case sqlmapping: SQLMapping => {
        sqlmapping.sqlContents
      }
      case _ => ""
    }
    s"""
        |{
        |  "entities": [
        |    {
        |      "typeName": "Table",
        |      "attributes": {
        |        "name": "$outputTable",
        |        "qualifiedName": "$qualifiedName",
        |        "description": "$sql"
        |      }
        |    }
        |  ]
        |}
        |""".stripMargin
  }

  def generateBodyJson(mapping: Config): String = {
    val inputTables = getSourceTableNameList(mapping)
    val outputTable = getOutputTableName(mapping)
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
  catch
  {
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