package com.metabolic.data.mapper.app

import com.amazonaws.regions.Regions
import com.metabolic.data.core.services.spark.partitioner.{DatePartitioner, Repartitioner, SchemaManagerPartitioner}
import com.metabolic.data.core.services.spark.transformations.FlattenTransform
import com.metabolic.data.core.services.spark.writer.partitioned_file.{CSVPartitionWriter, DeltaPartitionWriter, JSONPartitionWriter, ParquetPartitionWriter}
import com.metabolic.data.core.services.spark.writer.stream.KafkaWriter
import com.metabolic.data.mapper.domain.io.EngineMode.EngineMode
import com.metabolic.data.mapper.domain.io._
import com.metabolic.data.mapper.domain.ops.SinkOp
import com.metabolic.data.mapper.domain.ops.sink._
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object MetabolicWriter extends Logging {

  private def prepareOutput(sink: Sink, output: DataFrame) = {
    sink.ops
      .foldLeft(output) { (df: DataFrame, op: SinkOp) =>
        op match {
          case flatten: FlattenSinkOp => {
            df
              .transform(new FlattenTransform().flatten(flatten.column))
          }
          case _ => { df}
        }

      }
  }


  private def prepareSink(sink: Sink)(implicit spark: SparkSession): Repartitioner = {

    sink.ops
      .foldLeft(Repartitioner(Seq.empty, Seq.empty)) { (r: Repartitioner, op: SinkOp) =>

        op match {
          case schema: ManageSchemaSinkOp => {

            val schemaPartitioner = new SchemaManagerPartitioner("default", sink.name)

            r.addColumnsWithBuilder(schemaPartitioner.partitionColumnNames, schemaPartitioner)

          }

          case date: DatePartitionSinkOp => {

            val datePartitioner = new DatePartitioner(Option.apply(date.eventTimeColumnName), date.depth)

            r.addColumnsWithBuilder(datePartitioner.partitionColumnNames, datePartitioner)

          }

          case explicit: ExplicitPartitionSinkOp => {

            r.addColumns(explicit.partitionColumns)

          }

          case _ => r
        }
      }
  }

  def write(df: DataFrame, sink: Sink, historical: Boolean, autoSchema: Boolean, checkpointPath: String, mode: EngineMode, namespaces: Seq[String])
           (implicit spark: SparkSession, region: Regions) = {

    val _df = prepareOutput(sink, df)

    sink match {

      case streamSink: StreamSink => {
        streamSink.format match {

          case IOFormat.KAFKA =>
            logger.info(s"Writing Kafka sink ${streamSink.topic}")

            new KafkaWriter(streamSink.servers, streamSink.apiKey, streamSink.apiSecret,
              streamSink.topic, streamSink.idColumnName, checkpointPath)
              .write(_df, mode)

        }
      }
      case fileSink: FileSink => {

        val path = if (autoSchema) {
          val versionRegex = """(.*)/(version=\d+/)""".r
          versionRegex.replaceAllIn(fileSink.path, "$1")
        } else {
          fileSink.path
        }

        val fileSaveMode = if(historical) { SaveMode.Overwrite} else { fileSink.saveMode }

        val repartitioner = prepareSink(sink)(_df.sparkSession)

        val _output = _df.transform(repartitioner.repartition())
        fileSink.format match {

          case IOFormat.CSV =>
            new CSVPartitionWriter(repartitioner.partitionColumnNames, path, fileSaveMode, checkpointPath)
              .write(_output, mode)

          case IOFormat.JSON =>
            new JSONPartitionWriter(repartitioner.partitionColumnNames, path, fileSaveMode, checkpointPath)
              .write(_output, mode)

          case IOFormat.PARQUET =>
            new ParquetPartitionWriter(repartitioner.partitionColumnNames, path, fileSaveMode, checkpointPath)
              .write(_output, mode)

          case IOFormat.DELTA =>
            new DeltaPartitionWriter(repartitioner.partitionColumnNames, path, fileSaveMode,
              fileSink.eventTimeColumnName, fileSink.idColumnName, fileSink.upsert,
              fileSink.dbName, checkpointPath, namespaces)
              .write(_output, mode)

        }
      }
    }
  }

}
