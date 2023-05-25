package com.metabolic.data.core.services.spark.transformations

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame}

class FlattenTransform()
 extends Logging {

   def flatten(columnName: Option[String]): DataFrame => DataFrame = { df =>

     columnName match {
       case Some(column) =>
         val oldColumns = df.columns.filter(_ != column).map(col(_))
         val flattenedColumns = flattenStructSchema(df.select(column).schema, forColumnName = columnName)

         df.select((oldColumns++flattenedColumns):_*)

       case None =>     df.select(flattenStructSchema(df.schema, forColumnName = columnName):_*)
     }

  }


  private def flattenStructSchema(schema: StructType, prefix: String = null, forColumnName: Option[String]): Array[Column] = {
    schema.fields.flatMap(f => {
      val columnName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenStructSchema(st, columnName, forColumnName)
        case _ => {
          forColumnName match {
            case Some(column) => {
              Array(col(columnName)
                .as(columnName
                  .replace(column + ".", "")
                  .replace(".", "_")))
            }
            case None => Array(col(columnName)
              .as(columnName
                .replace(".", "_")))
          }
        }
      }
    })
  }


}
