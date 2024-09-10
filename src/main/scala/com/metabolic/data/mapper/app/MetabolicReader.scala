package com.metabolic.data.mapper.app

import com.metabolic.data.core.services.spark.filter.{DateComponentsFromReader, DateComponentsUpToReader, DateFieldFromReader, DateFieldUpToReader}
import com.metabolic.data.core.services.spark.reader.file.{CSVReader, DeltaReader, JSONReader, ParquetReader}
import com.metabolic.data.core.services.spark.reader.stream.KafkaReader
import com.metabolic.data.core.services.spark.reader.table.{IcebergReader, TableReader}
import com.metabolic.data.core.services.spark.transformations._
import com.metabolic.data.mapper.domain.io.EngineMode.EngineMode
import com.metabolic.data.mapper.domain.io._
import com.metabolic.data.mapper.domain.ops._
import com.metabolic.data.mapper.domain.ops.source._
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

object MetabolicReader extends Logging {

 def read(source: Source, historical: Boolean, mode: EngineMode, enableJDBC: Boolean, queryOutputLocation: String, jobName: String)(implicit spark: SparkSession) = {

  val input = readSource(source, mode, spark, enableJDBC, queryOutputLocation, jobName)

  val prepared = prepareSource(source, historical, input)

  prepared.createOrReplaceTempView(source.name)

 }

 private def readSource(source: Source, mode: EngineMode, spark: SparkSession, enableJDBC: Boolean, queryOutputLocation: String, jobName: String) = {
  source match {

   case streamSource: StreamSource => {
    logger.info(s"Reading stream source ${streamSource.name} from ${streamSource.topic}")

    streamSource.format match {
     case IOFormat.KAFKA => new KafkaReader(streamSource.servers, streamSource.key, streamSource.secret, streamSource.topic, jobName)
       .read(spark, mode)
    }
   }

   case fileSource: FileSource => {
    logger.info(s"Reading file source ${fileSource.name} from ${fileSource.inputPath}")

    fileSource.format match {
     case IOFormat.CSV =>
      new CSVReader(fileSource.inputPath)
        .read(spark, mode)
     case IOFormat.JSON =>
      new JSONReader(fileSource.inputPath, fileSource.useStringPrimitives)
        .read(spark, mode)
     case IOFormat.PARQUET =>
      new ParquetReader(fileSource.inputPath)
        .read(spark, mode)
     case IOFormat.DELTA =>
      new DeltaReader(fileSource.inputPath)
        .read(spark, mode)
    }
   }

   case meta: TableSource => {
    logger.info(s"Reading source ${meta.fqn} already in metastore")

    new IcebergReader(meta.fqn).read(spark, mode)
   }
  }
 }

 private def prepareSource(source: Source, historical: Boolean, input: DataFrame) = {
  source.ops
    .foldLeft(input) { (df: DataFrame, op: SourceOp) =>

     op match {
      case filter: FilterSourceOp => {
       if (!historical) {
        df
          .transform(new DateFieldUpToReader(filter.onColumn, filter.toDate).filter())
          .transform(new DateFieldFromReader(filter.onColumn, filter.fromDate).filter())
       } else df
      }
      case prune: PruneDateComponentsSourceOp => {
       if (!historical) {
        df
          .transform(new DateComponentsUpToReader(prune.toDate, prune.depth).filter())
          .transform(new DateComponentsFromReader(prune.fromDate, prune.depth).filter())
       } else df
      }
      case dedupe: DedupeSourceOp => {
       df
         .transform(new DedupeTransform(dedupe.idColumns, dedupe.orderColumns).dedupe())
      }
      case demulti: DemultiSourceOp => {
       df
         .transform(new DemultiplexTransform(demulti.idColumns, demulti.orderColumns, demulti.dateColumn, demulti.format)
           .demultiplex(demulti.from, demulti.to, demulti.endOfMonth))
      }
      case expr: SelectExpressionSourceOp => {
       df
         .transform(new ExpressionTransformation(expr.expressions).apply())
      }
      case drop: DropExpressionSourceOp => {
       df
         .transform(new DropTransformation(drop.columns).apply())
      }
      case flatten: FlattenSourceOp => {
       df
         .transform(new FlattenTransform().flatten(flatten.column))
      }
      case watermark: WatermarkSourceOp => {
       df
         .withWatermark(watermark.onColumn, watermark.value)
      }

     }
    }
 }

}
