package com.metabolic.data.core.services.schema

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.functions.{col, lit, max}
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

class SchemaService(implicit spark: SparkSession) extends Logging{

  val versionColumnName: String = "version"
  val initialVersionValue: Integer = 1
  val versionRegex = """(.*)_v(\d)+""".r

  def getTable(dbName: String, tableName: String)(implicit spark: SparkSession): DataFrame = {

    val canonicalTableName = s"${dbName}.${tableName}"

    if (spark.catalog.tableExists(canonicalTableName)) {
      logger.info(s"Getting schema for table ${canonicalTableName}")
      castVersionToInt(spark.table(canonicalTableName))
    } else {
      logger.info(s"Register empty schema for ${canonicalTableName}")
      spark.emptyDataFrame
    }

  }

  def diffSchemas(left: Set[StructField], right: Set[StructField]): Array[StructField] = {
    val cleanLeft = left
          .map { f => StructField(versionRegex.replaceAllIn(f.name, "$1"), f.dataType)}

    right
      .map { f => StructField(versionRegex.replaceAllIn(f.name, "$1"), f.dataType, f.nullable)}
      .foldLeft(Set[StructField]()) { (accum: Set[StructField], element: StructField) =>
        if (cleanLeft.contains(StructField(element.name, element.dataType))) {
          accum
        } else {
          accum ++ Set(element)
        }
      }.toArray
  }

  def addVersioningToSchemaColumns(oldSchema: StructType, newDf: DataFrame): DataFrame = {
    val cleanOld = oldSchema
          .fields.foldLeft(Map[StructField, Int]()){ (m:Map[StructField, Int], s:StructField) => m + (StructField(versionRegex.replaceAllIn(s.name, "$1"), s.dataType, s.nullable)
            -> versionRegex
          .findFirstMatchIn(s.name)
          .map(_.group(2).toInt).getOrElse(0))
        }

      newDf.schema.fields.foldLeft(newDf) { (accum: DataFrame, elm: StructField) =>
        if (cleanOld.keySet.contains(elm) && cleanOld.get(elm).map(_.toInt).get > 0) {
          accum.withColumnRenamed(elm.name, s"${elm.name}_v${cleanOld.get(elm).map(_.toInt).get}")
        } else {
          accum
        }
    }

  }

  def getVersion(dbName: String, tableName: String): Integer = {

    val currentTable = getTable(dbName, tableName)(spark)

    if(containsVersion(currentTable.schema)) {
      getMaxVersion(currentTable)
    } else {
      initialVersionValue
    }
  }


  def getMaxVersion(df: DataFrame): Integer = {

    castVersionToInt(df)
      .select(max(versionColumnName))
      .first()
      .getInt(0)

  }

  def containsVersion(schema: StructType): Boolean = {

    schema
      .contains(StructField(versionColumnName, IntegerType)) | schema.contains(StructField(versionColumnName, StringType))

  }

  def getTableSchema(dbName: String, tableName: String): StructType = {

    getTable(dbName, tableName)
      .schema

  }

  def flattenSchema(schema: StructType, prefix: String = null) : Array[Column] = {
    // Convert structType columns to Array
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenSchema(st, colName)
        case _ => Array(col(colName))
      }
    })
  }

  /**
   *
   * I have a table (Companies) in the Data Lake, And I force it (through the path) to version = 1.
   *
   * No changes, keep version
   * Any schema changes, version+1
   *
   *  A new table (Companies) has a new column, delete column
   *  Nothing else to do at path level.
   *
   *  A new table (Companies) changed column name.
   *  Double check wasn't already beeing used, but nothing to do.
   *
   * A new table (Companies) changed a field "mrr" from string to float.
   * rename field mrr to mrr_v2
   *
   * @param oldSchema
   * @param currentVersion
   * @param df
   * @return
   */
  def compareSchema(oldSchema: StructType, currentVersion: Int, df: DataFrame): DataFrame = {

    val newSchema = castVersionToInt(df).schema
//
//    val diff = newSchema
//      .fields
//      .diff(oldSchema.fields)

    val diff = diffSchemas(oldSchema.fields.toSet, newSchema.fields.toSet)

    if(diff.isEmpty) {
      if (containsVersion(newSchema)) {
        logger.info(s"Register schema with existing version column")
        df
      } else  {
        logger.info(s"Register schema with added version column")
        addVersion(df, currentVersion)
      }
    } //else if (StructType(diff).sortBy(_.name) == newSchema.sortBy(_.name)) {
      else if (oldSchema.isEmpty) {
      logger.info(s"Register new added schema with added version column")
      addVersion(df, initialVersionValue)
    } else {
      logger.info(s"Existing schema and new schema has difference as ${StructType(diff)}")

      val dfWithVersion = addVersion(df, currentVersion + 1)

      val diffFlattenedSchema = flattenSchema(StructType(diff))
      val oldFlattenedSchema = flattenSchema(oldSchema)

      diffFlattenedSchema
          .filter((c: Column) => oldFlattenedSchema.contains(c))
          .foldLeft(dfWithVersion)(renameColumnWithVersion(currentVersion + 1))
    }

  }

  private def findDiffOrder(oldSchema: StructType, newSchema: StructType): (StructType, StructType) = {
    if (newSchema.fields.length >= oldSchema.fields.length) {
      (newSchema, oldSchema)
    }else{
      (oldSchema, newSchema)
    }
  }

  private def castVersionToInt(df: DataFrame) = {
    if(containsVersion(df.schema)) {
      df.withColumn(versionColumnName + "_old", df(versionColumnName).cast(IntegerType))
        .drop(versionColumnName)
        .withColumnRenamed(versionColumnName + "_old", versionColumnName)
    }else{
      df
    }
  }

  def compareSchema(dbName: String, tableName: String, incomingData: DataFrame): DataFrame = {
    val oldSchema = getTableSchema(dbName, tableName)
    val currentVersion = getVersion(dbName, tableName)
    logger.info(s"Current version for ${tableName} is ${currentVersion}")
    // Convert version to Int and retrieve the historical versions for column changes
    val transformedIncomingData = addVersioningToSchemaColumns(oldSchema, makeColumnsMetastoreCompatible(incomingData))
    compareSchema(oldSchema, currentVersion, transformedIncomingData)

  }

  private def renameColumnWithVersion(nextVersion: Int): (DataFrame, Column) => DataFrame = {
    (accumulator: DataFrame, element: Column) =>
      //The regex replacement is to prevent double versioning. ie id_v4_v5
      logger.info(s"Increase version for column ${element.toString()} to ${nextVersion}")
      accumulator.withColumnRenamed(element.toString(), s"${versionRegex.replaceAllIn(element.toString(), "$1")}_v$nextVersion")

  }

  private def addVersion(df: DataFrame, version: Int) = {
    df
      .withColumn(versionColumnName, lit(version).cast(IntegerType))
  }

  def sanitize(name: String) = {
    name
      .replaceAll("-", "_")
      .replaceAll("%", "_")
      .replaceAll("&", "_")
      .replaceAll("\"", "_")
      .replaceAll(" ", "_")
      .toLowerCase
  }

  def safeStruct(unsafeSchema: StructType): StructType = {

    StructType(
      unsafeSchema.fields.map { f =>

        f.dataType match {
          case s: StructType => StructField(sanitize(f.name), StructType(safeStruct(StructType(s.sortBy(_.name)))))
          case a: ArrayType => StructField(sanitize(f.name), ArrayType(safeArrayElement(a.elementType)))
          case g => StructField(sanitize(f.name), g)
        }
      }
    )

  }

  def safeArrayElement(unsafeType: DataType): DataType = {

    unsafeType match {
      case s: StructType => safeStruct(s)
      case a: ArrayType => ArrayType(safeArrayElement(a.elementType))
      case g => g
    }

  }

  def makeColumnsMetastoreCompatible(inputDf: DataFrame) = {
    inputDf.select(
      inputDf.schema.fields.map { f =>

        val safeName = sanitize(f.name)

        f.dataType match {

          case s: StructType =>
            col(s"`${f.name}`")
              .cast(s"${safeStruct(s).sql}")
              .name(safeName)

          case a: ArrayType =>
            col(s"`${f.name}`")
              .cast(s"${ArrayType(safeArrayElement(a.elementType)).sql}")
              .name(safeName)

          case _ => /* Any other datatype */
            col(s"`${f.name}`")
              .name(safeName)
        }
      }: _*
    )

  }
}
