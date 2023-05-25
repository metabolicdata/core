package com.metabolic.data.core.services.spark.writer.stream

import com.amazonaws.auth._
import com.amazonaws.services.kinesis._
import com.amazonaws.services.kinesis.model._
import org.apache.spark.sql.ForeachWriter

import java.nio.ByteBuffer
import scala.collection.mutable.ArrayBuffer

/**
 * A simple Sink that writes to the given Amazon Kinesis `stream` in the given `region`. For authentication, users may provide
 * `awsAccessKey` and `awsSecretKey`, or use IAM Roles when launching their cluster.
 *
 * This Sink takes a two column Dataset, with the columns being the `partitionKey`, and the `data` respectively.
 * We will buffer data up to `maxBufferSize` before flushing to Kinesis in order to reduce cost.
 */
class cKinesisForEachWriter(
                   stream: String,
                   region: String,
                   awsAccessKey: Option[String] = None,
                   awsSecretKey: Option[String] = None) extends ForeachWriter[(String, Array[Byte])] {

  // Configurations
  private val maxBufferSize = 500 * 1024 // 500 KB

  private var client: AmazonKinesis = _
  private val buffer = new ArrayBuffer[PutRecordsRequestEntry]()
  private var bufferSize: Long = 0L

  override def open(partitionId: Long, version: Long): Boolean = {
    client = createClient
    true
  }

  override def process(value: (String, Array[Byte])): Unit = {
    val (partitionKey, data) = value
    // Maximum of 500 records can be sent with a single `putRecords` request
    if ((data.length + bufferSize > maxBufferSize && buffer.nonEmpty) || buffer.length == 500) {
      flush()
    }
    buffer += new PutRecordsRequestEntry().withPartitionKey(partitionKey).withData(ByteBuffer.wrap(data))
    bufferSize += data.length
  }

  override def close(errorOrNull: Throwable): Unit = {
    if (buffer.nonEmpty) {
      flush()
    }
    client.shutdown()
  }

  /** Flush the buffer to Kinesis */
  private def flush(): Unit = {
    val recordRequest = new PutRecordsRequest()
      .withStreamName(stream)
      .withRecords(buffer: _*)

    client.putRecords(recordRequest)
    buffer.clear()
    bufferSize = 0
  }

  /** Create a Kinesis client. */
  private def createClient: AmazonKinesis = {
    val cli = if (awsAccessKey.isEmpty || awsSecretKey.isEmpty) {
      AmazonKinesisClientBuilder.standard()
        .withRegion(region)
        .build()
    } else {
      AmazonKinesisClientBuilder.standard()
        .withRegion(region)
        .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsAccessKey.get, awsSecretKey.get)))
        .build()
    }
    cli
  }
}
