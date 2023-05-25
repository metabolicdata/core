package com.metabolic.data.core.services.catalogue

import com.metabolic.data.core.services.util.ConfigUtilsService
import com.metabolic.data.mapper.domain.Config
import com.metabolic.data.mapper.domain.io._
import org.apache.logging.log4j.scala.Logging

import java.math.BigInteger
import java.security.MessageDigest
import scala.collection.mutable

class AtlanService(token: String) extends Logging {

  val versionRegex = """version=(\d)+/""".r

  def setLineage(mapping: Config): String = {
    val body = generateBodyJson(mapping)
    logger.info(s"Atlan Json Body ${body}")
    HttpRequestHandler.sendHttpPostRequest("https://factorial.atlan.com/api/meta/entity/bulk#createProcesses", body, token)
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