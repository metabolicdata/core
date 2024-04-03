package com.metabolic.data.mapper.app

import com.amazonaws.regions.Regions
import com.metabolic.data.core.services.athena.AthenaAction
import com.metabolic.data.core.services.catalogue.AtlanCatalogueAction
import com.metabolic.data.core.services.glue.GlueCrawlerAction
import com.metabolic.data.core.services.spark.udfs.MetabolicUserDefinedFunction
import com.metabolic.data.core.services.util.ConfigReaderService
import com.metabolic.data.mapper.domain._
import com.metabolic.data.mapper.services.{AfterAction, ConfigParserService}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import java.util.jar.JarFile
import scala.collection.JavaConverters._
import scala.reflect.internal.util.ScalaClassLoader.URLClassLoader


class MetabolicApp(sparkBuilder: SparkSession.Builder) extends Logging {

  def run(configPath: String, params: Map[String, String]): Unit = {

    implicit val region = Regions.fromName(params("dp.region"))

    val rawConfig = new ConfigReaderService()
      .getConfig(configPath, params)

    val configs = new ConfigParserService()
      .parseConfig(rawConfig)

    implicit val spark = sparkBuilder
      .appName(s" Metabolic Mapper - $configPath")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
      .config("spark.databricks.delta.optimize.repartition.enabled", "true")
      .config("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")
      .getOrCreate()

    params.get("configJar") match {
      case Some(configJar) => loadUDFs(configJar)
      case None =>
    }

    val actions = Seq[AfterAction](
      new AthenaAction(),
      new GlueCrawlerAction(),
      new AtlanCatalogueAction()
    )

    transformAll(configs, actions)

  }

  def isAssignableFromMyUDF(loadedClass: Class[_]): Boolean = {
    loadedClass.getInterfaces.exists(_ == classOf[MetabolicUserDefinedFunction]) || (loadedClass.getSuperclass != null && isAssignableFromMyUDF(loadedClass.getSuperclass))
  }

  private def loadUDFs(udfJarPath: String)(implicit spark: SparkSession): Unit = {

    logger.info(f"Loading UDFs from $udfJarPath")
    val jarFile = new JarFile(udfJarPath)
    val classNames = jarFile.entries().asScala
      .filter(_.getName.endsWith(".class"))
      .map(_.getName.replace("/", ".").stripSuffix(".class"))
      .toList

    val udfJarURL = new File(udfJarPath).toURI.toURL
    val classLoader = new URLClassLoader(Array(udfJarURL), getClass.getClassLoader)

    classNames.foreach { className =>
      val loadedClass = classLoader.loadClass(className)
      if (isAssignableFromMyUDF(loadedClass)) {
        val udfInstance = loadedClass.getConstructor().newInstance().asInstanceOf[MetabolicUserDefinedFunction]
        udfInstance.register(spark)
        logger.info(s"Registered UDF: ${udfInstance.name} from $className")
      }
    }

  }

  def transformAll(configs: Seq[Config], withAction: Seq[AfterAction] = Seq.empty[AfterAction])(implicit region: Regions, spark: SparkSession): Unit = {

    configs.foldLeft(Seq[StreamingQuery]()) { (streamingQueries, config) =>
      before(config)
      logger.info(s"Transforming ${config.name}")
      val streamingQuery = transform(config)
      logger.info(s"Done with ${config.name}")
      after(config, withAction)
      logger.info(s"Done registering ${config.name}")
      streamingQueries ++ streamingQuery
    }.par.foreach {
      _.awaitTermination
    }

  }

  def before(config: Config) = {}

  def transform(mapping: Config)(implicit spark: SparkSession, region: Regions): Seq[StreamingQuery] = {

    mapping.sources.foreach { source =>
      MetabolicReader.read(source, mapping.environment.historical, mapping.environment.mode, mapping.environment.enableJDBC, mapping.environment.queryOutputLocation, mapping.getCanonicalName)
    }

    mapping.mappings.foreach { mapping =>
      MetabolicMapper.map(spark, mapping)
    }

    val output: DataFrame = spark.table("output")

    MetabolicWriter.write(output, mapping.sink, mapping.environment.historical, mapping.environment.autoSchema,
      mapping.environment.baseCheckpointLocation, mapping.environment.mode, mapping.environment.namespaces)

  }

  def after(mapping: Config, actions: Seq[AfterAction])(implicit region: Regions) = {

    actions.foreach(_.run(mapping))

  }

}

object MetabolicApp {

  def apply() = {
    new MetabolicApp(SparkSession.builder())
  }
}

