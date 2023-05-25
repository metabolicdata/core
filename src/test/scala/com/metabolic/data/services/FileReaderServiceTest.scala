package com.metabolic.data.services

import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3.{AmazonS3ClientBuilder, AmazonS3URI}
import org.scalatest.funsuite.AnyFunSuite

class FileReaderServiceTest extends AnyFunSuite {

  ignore("s3 download") {

    val path = "s3://factorial-etl/entity_mapper/mappings/employees.sql"

    val awsCredentials = new DefaultAWSCredentialsProviderChain()
    val s3Client = AmazonS3ClientBuilder.standard()
      .withCredentials(awsCredentials)
      .withRegion("eu-central-1").build()
    val s3Uri = new AmazonS3URI(path)

    try {
      val fullObject = s3Client.getObject(s3Uri.getBucket, s3Uri.getKey)
    } catch {
      case e: AmazonServiceException => {

        println(s"AmazonServiceException (likely AmazonS3Exception), ${e.getMessage} for key ${path}")

      }
    }
  }
}
