package com.metabolic.data.core.services.util


import com.metabolic.data.mapper.domain.config.Config
import com.typesafe.config.Config
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import com.metabolic.data.mapper.domain.io.{FileSink, StreamSink}

object ConfigUtilsService {

  val periodFormatter = DateTimeFormat.forPattern("yyyy-MM")
  val dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
  val datetimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss")
  val isoDatetimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

  def getDateTime(forKey: String, config: com.typesafe.config.Config): DateTime = {

    val value = config.getString(forKey)
    datetimeFormatter.parseDateTime(value)

  }

  def getDate(forKey: String, config: com.typesafe.config.Config): DateTime = {

    val value = config.getString(forKey)
    dateFormatter.parseDateTime(value)

  }

  def getPeriod(forKey: String, config: com.typesafe.config.Config): DateTime = {

    val value = config.getString(forKey)
    periodFormatter.parseDateTime(value)

  }


  def getTablePrefix(namespaces: Seq[String], s3Path: String): String = {
    namespaces
      .flatMap { namespace =>
        if (s3Path.contains(namespace)) {
          namespace + "_"
        } else {
          ""
        }
      }.mkString("")
  }

  def getTableInfix(namespaces: Seq[String], s3Path: String): String = {
    namespaces
      .map { namespace =>
        val words = namespace.split("_", 2)
          if (containsWords(s3Path, words)) {
            Some(words(1) + "_")
          } else {
            None
          }
      }
      .filter(_.isDefined)
      .map(_.get)
      .mkString("")
  }
  private def containsWords(str: String, words: Array[String]): Boolean = {
    words.forall(word => str.contains(word))
  }
  def getTableSuffix(namespaces: Seq[String], s3Path: String): String = {
    namespaces
      .flatMap { namespace =>
        if (s3Path.contains(namespace)) {
          "_" + namespace
        } else {
          ""
        }
      }.mkString("")
  }
  def getTableName(mapping: com.metabolic.data.mapper.domain.config.Config): String = {

    mapping.sink match {
      case f: FileSink => {
        getTableNameFileSink(f.path)
      }

      case s: StreamSink => {
        getTableNameFileStream(s.topic)
      }
    }
  }

  def getTableNameFileSink(path: String): String = {
    path
      .replaceAll("version=[0-9]*/", "")
      .split("/").last
  }

  def getTableNameFileStream(topic: String): String = {
    topic
      .replaceAll("version_[0-9]*.", "")
    //.split(".").last
  }

  def getDataBaseName(path: String): String = {
    val index = path.lastIndexOf("/")
    val result = if (index == -1) path else path.substring(0, index)
    result
  }

}