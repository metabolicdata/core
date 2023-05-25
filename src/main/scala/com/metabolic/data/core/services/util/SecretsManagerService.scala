package com.metabolic.data.core.services.util

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder
import com.amazonaws.services.secretsmanager.model.{GetSecretValueRequest, GetSecretValueResult}
import org.apache.logging.log4j.scala.Logging

import java.util.Base64
import net.liftweb.json._

class SecretsManagerService(implicit val region: Regions) extends Logging {

  val awsCredentials = new DefaultAWSCredentialsProviderChain()

  val smClient = AWSSecretsManagerClientBuilder.standard
    .withRegion(region)
    .withCredentials(awsCredentials)
    .build()

  def get(secretName: String): String = {

    val getSecretValueRequest = new GetSecretValueRequest()
      .withSecretId(secretName)
      .withVersionStage("AWSCURRENT")

    smClient.getSecretValue(getSecretValueRequest) match {
      case r: GetSecretValueResult => {
        if (r.getSecretString != null) {
          r.getSecretString
        } else {
          new String(Base64.getDecoder.decode(r.getSecretBinary).array)
        }
      }
      case _ => {
        logger.info("The requested secret " + secretName + " was not found")
          ""
      }
    }

  }

  def parseDict[T](dict: String)(implicit m: Manifest[T]): T = {

    implicit val formats = DefaultFormats

    parse(dict).extract[T]

  }

}
