package com.metabolic.data.core.services.glue

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.glue._
import com.amazonaws.services.glue.model._
import org.apache.logging.log4j.scala.Logging

class GlueCrawlerService(implicit val region: Regions) extends Logging {

  val awsCredentials = new DefaultAWSCredentialsProviderChain()

  val glueClient = AWSGlueClientBuilder.standard()
    .withCredentials(awsCredentials)
    .withRegion(region)
    .build


  def createAndRunCrawler(iam: String, s3Paths: Seq[String], dbName: String, crawlerName: String,
                                  prefix: String): Unit = {
    try {
      createCrawler(iam, s3Paths, dbName, crawlerName, prefix)
      runCrawler(crawlerName)

    } catch {
      case _: AlreadyExistsException => {
        updateCrawler(crawlerName, s3Paths, dbName)
        runCrawler(crawlerName)

      }
      case e: Exception =>
        logger.error(e.getMessage)
        logger.warn(crawlerName + " failed. Please run it manually. ")
    } finally {
    }
  }

  private def waitForCrawler(name: String): Boolean = {

    val metricsRequest = new GetCrawlerMetricsRequest()
      .withCrawlerNameList(name)

    val metricRespose = glueClient.getCrawlerMetrics(metricsRequest)

    val crawlerMetric = metricRespose.getCrawlerMetricsList.get(0)

    if (crawlerMetric.isStillEstimating) {
      Thread.sleep(5*1000L)
      waitForCrawler(name)
    } else if (crawlerMetric.getTimeLeftSeconds  > 0.0) {
      Thread.sleep(crawlerMetric.getTimeLeftSeconds.toLong*1000L)
      waitForCrawler(name)
    }
    true
  }

  private def createCrawler(iam: String, s3Paths: Seq[String], dbName: String, crawlerName: String, prefix: String) = {

    val s3TargetList = s3Paths
      .map {
        new S3Target().withPath(_)
      }

    val targets = new CrawlerTargets()
      .withS3Targets(s3TargetList:_*)

    val recrawlPolicy = new RecrawlPolicy()
      .withRecrawlBehavior(RecrawlBehavior.CRAWL_NEW_FOLDERS_ONLY)

    val schemaChangePolicy = new SchemaChangePolicy()
      .withUpdateBehavior(UpdateBehavior.UPDATE_IN_DATABASE)
      .withDeleteBehavior(DeleteBehavior.DEPRECATE_IN_DATABASE)

    val crawlerCreateRequest = new CreateCrawlerRequest()
      .withDatabaseName(dbName)
      .withName(crawlerName)
      .withTablePrefix(prefix)
      .withDescription("Created by Metabolic using the AWS Glue Java API")
      .withTargets(targets)
      .withRole(iam)
      .withRecrawlPolicy(recrawlPolicy)
      .withSchemaChangePolicy(schemaChangePolicy)
      .withTags(Map("Owner" -> "Data",  "Environment" -> environment).asJava)

    glueClient.createCrawler(crawlerCreateRequest)
    logger.info(crawlerName + " was successfully created")
  }

  def runCrawler(crawlerName: String) = {

    val crawlerStartRequest = new StartCrawlerRequest()
      .withName(crawlerName)

    glueClient.startCrawler(crawlerStartRequest)
    logger.info(crawlerName + " was successfully started")

  }

  private def deleteCrawler(crawlerName: String) = {

    val crawlerDeleteRequest = new DeleteCrawlerRequest()
      .withName(crawlerName)

    glueClient.deleteCrawler(crawlerDeleteRequest)
    logger.info(crawlerName + " was successfully deleted")
  }

  private def updateCrawler(crawlerName: String, newPaths: Seq[String], database: String) = {

    val s3TargetList = newPaths
      .map {
        new S3Target().withPath(_)
      }

    val targets = new CrawlerTargets()
      .withS3Targets(s3TargetList:_*)

    val crawlerUpdateRequest = new UpdateCrawlerRequest()
      .withName(crawlerName)
      .withDatabaseName(database)
      .withTargets(targets)

    glueClient.updateCrawler(crawlerUpdateRequest)

    logger.info(crawlerName + " was successfully updated")
  }

  def checkStatus(name: String): CrawlerMetrics = {

    val request = new GetCrawlerMetricsRequest()
      .withCrawlerNameList(name)

    val response = glueClient.getCrawlerMetrics(request)

    response.getCrawlerMetricsList.get(0)

  }

}
