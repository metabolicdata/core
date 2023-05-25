package com.metabolic.data.core.services.spark.partitioner

import com.metabolic.data.core.services.spark.filter.DataLakeDepth._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class DatePartitioner(val dateColumnName: Option[String],
                      val depth: DataLakeDepth)
  extends PartitionBuilder {

  private val yearColumnName = "yyyy"
  private val monthColumnName = "mm"
  private val dayColumnName = "dd"
  private val hourColumnName = "hh"
  private val minuteColumnName = "mi"

  val partitionColumnNames: Seq[String] = {

    dateColumnName match {
      case Some(_) => {
        depth match {
          case YEAR => Seq(yearColumnName)
          case MONTH => Seq(yearColumnName, monthColumnName)
          case DAY => Seq(yearColumnName, monthColumnName, dayColumnName)
          case HOUR => Seq(yearColumnName, monthColumnName, dayColumnName, hourColumnName)
          case MINUTE => Seq(yearColumnName, monthColumnName, dayColumnName, hourColumnName, minuteColumnName)
        }
      }
      case None => Seq.empty[String]
    }

  }

  override def addPartition(): DataFrame => DataFrame = { df =>

      dateColumnName match {
        case Some(date) =>

          depth match {
            case YEAR => df
              .withColumn(yearColumnName, format_string("%04d", year(df.col(date))))
            case MONTH => df
              .withColumn(yearColumnName, format_string("%04d", year(df.col(date))))
              .withColumn(monthColumnName, format_string("%02d", month(df.col(date))))
            case DAY => df
              .withColumn(yearColumnName, format_string("%04d", year(df.col(date))))
              .withColumn(monthColumnName, format_string("%02d", month(df.col(date))))
              .withColumn(dayColumnName, format_string("%02d", dayofmonth(df.col(date))))
            case HOUR => df
              .withColumn(yearColumnName, format_string("%04d", year(df.col(date))))
              .withColumn(monthColumnName, format_string("%02d", month(df.col(date))))
              .withColumn(dayColumnName, format_string("%02d", dayofmonth(df.col(date))))
              .withColumn(hourColumnName, format_string("%02d", hour(df.col(date))))
            case MINUTE => df
              .withColumn(yearColumnName, format_string("%04d", year(df.col(date))))
              .withColumn(monthColumnName, format_string("%02d", month(df.col(date))))
              .withColumn(dayColumnName, format_string("%02d", dayofmonth(df.col(date))))
              .withColumn(hourColumnName, format_string("%02d", hour(df.col(date))))
              .withColumn(minuteColumnName, format_string("%02d", minute(df.col(date))))
          }

        case None => df
      }
    }


}
