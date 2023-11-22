package com.metabolic.data.mapper.app

import com.amazonaws.regions.Regions
import com.metabolic.data.core.services.catalogue.AtlanService
import com.metabolic.data.core.services.glue.{AthenaCatalogueService, GlueCrawlerService}
import com.metabolic.data.core.services.spark.udfs.MetabolicUserDefinedFunction
import com.metabolic.data.core.services.util.{ConfigReaderService, ConfigUtilsService}
import com.metabolic.data.mapper.domain._
import com.metabolic.data.mapper.domain.io._
import com.metabolic.data.mapper.services.ConfigParserService
import org.apache.logging.log4j.scala.Logging
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

    spark.sparkContext.setLogLevel("ERROR")

    params.get("configJar") match {
      case Some(configJar) => loadUDFs(configJar)
      case None =>
    }

    transformAll(configs)

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

  def transformAll(configs: Seq[Config])(implicit region: Regions, spark: SparkSession): Unit = {

    configs.foreach { config =>
      before(config)
      logger.info(s"Transforming ${config.name}")
      transform(config)
      logger.info(s"Done with ${config.name}")
      after(config)
      logger.info(s"Done registering ${config.name}")
    }

  }

  def before(config: Config) = {}

  def transform(mapping: Config)(implicit spark: SparkSession, region: Regions): Unit = {

    mapping.sources.foreach { source =>
      MetabolicReader.read(source, mapping.environment.historical, mapping.environment.mode, mapping.environment.enableJDBC, mapping.environment.queryOutputLocation)
    }

    mapping.mappings.foreach { mapping =>
      MetabolicMapper.map(spark, mapping)
    }

    val output: DataFrame = spark.table("output")

    val streamingQuery = MetabolicWriter.write(output, mapping.sink, mapping.environment.historical, mapping.environment.autoSchema,
      mapping.environment.baseCheckpointLocation, mapping.environment.mode, mapping.environment.namespaces)

    streamingQuery.map { _.awaitTermination }

  }

  def after(mapping: Config)(implicit region: Regions) = {
    logger.info(s"Done with ${mapping.name}, registering in Glue Catalog")
    register(mapping)

    if (mapping.sink.isInstanceOf[FileSink]) {
      logger.info(s"Done with ${mapping.name}, pushing lineage to Atlan")
      mapping.environment.atlanToken match {
        case Some(token) => {
          val atlan = new AtlanService(token, mapping.environment.atlanBaseUrl.getOrElse(""))
          atlan.setLineage(mapping)
          atlan.setMetadata(mapping)
        }
        case _ => ""
      }
    } else {
      logger.info(s"Done with ${mapping.name}")
    }
  }

  def register(config: Config)(implicit region: Regions): Unit = {

    val options = config.environment
    if (options.crawl && config.sink.isInstanceOf[FileSink]) {
      val name = s"${options.name} EM ${config.name}"
      val s3Path = config.sink.asInstanceOf[FileSink].path
        .replace("version=1/", "")

      val dbName = options.dbName
      val prefix = ConfigUtilsService.getTablePrefix(options.namespaces, s3Path)
      val tableName = prefix+ConfigUtilsService.getTableName(config)

      new AthenaCatalogueService().dropView(dbName, tableName)

      config.sink.format match {
        case IOFormat.DELTA => new AthenaCatalogueService().createDeltaTable(dbName, tableName, s3Path)
        case _ => new GlueCrawlerService().register(dbName, options.iamRole, name, Seq(s3Path), prefix)
      }
//      Schema Separation
//      if(options.name.contains("production")){
//        val suffix = ConfigUtilsService.getTableSuffix(options.namespaces, s3Path)
//        config.sink.format match {
//          case IOFormat.DELTA => new AthenaCatalogueService().createDeltaTable(options.dbName+suffix, ConfigUtilsService.getTableName(config), s3Path)
//          case _ => new GlueCrawlerService().register(options.dbName+suffix, options.iamRole, name+" "+options.dbName+suffix, Seq(s3Path), "")
//        }
//      }
    }
  }

}

object MetabolicApp {

  def apply() = {
    new MetabolicApp(SparkSession.builder())
  }
}

