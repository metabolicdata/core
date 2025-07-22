package com.metabolic.data.core.services.util

import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3ClientBuilder, AmazonS3URI}
import org.apache.logging.log4j.scala.Logging

import scala.io.Source

class FileReaderService(implicit val region: Regions) extends Logging {

  val awsCredentials = new DefaultAWSCredentialsProviderChain()

  val s3Client = AmazonS3ClientBuilder.standard
    .withCredentials(awsCredentials)
    .withRegion(region)
    .build

  object EmptyString {
    def unapply(s: String): Option[String] =
      if (s == null || s.trim.isEmpty) Some(s) else None
  }

  def getFileContents(path: String, separator: String = "\n"): String = {

    path match {
      case EmptyString(_) => ""
      case confPath => {
        if (confPath.startsWith("s3://")) readS3FileContents(confPath, separator)
        else if (confPath.startsWith("https://")) Source.fromURL(confPath).mkString
        else if (confPath.startsWith("http://")) Source.fromURL(confPath).mkString
        else readLocalFileContents(confPath, separator)
      }
    }


  }

  private def readS3FileContents(path: String, separator: String): String = {

    val s3Uri = new AmazonS3URI(path)

    try {
      val fullObject = s3Client.getObject(s3Uri.getBucket, s3Uri.getKey)
      Source.fromInputStream(fullObject.getObjectContent).getLines.mkString(separator)

    } catch {
      case e: AmazonServiceException => {
        logger.error(s"AmazonServiceException (likely AmazonS3Exception), ${e.getMessage} for key ${path}")
        throw e
      }
    }

  }

  private def readLocalFileContents(path: String, separator: String = "\n"): String = {

    Source.fromFile(path).getLines.mkString(separator)

  }
}
