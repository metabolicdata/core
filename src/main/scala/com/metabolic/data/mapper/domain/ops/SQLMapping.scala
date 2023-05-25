package com.metabolic.data.mapper.domain.ops

import com.amazonaws.regions.Regions
import com.metabolic.data.core.services.util.FileReaderService

trait SQLMapping extends Mapping {
  def sqlContents: String
}

class SQLFileMapping(path: String, implicit val region: Regions) extends SQLMapping {

  def sqlContents: String = {
    new FileReaderService()
      .getFileContents(path, " ")
      .trim()
      .replaceAll("\\s{2,}", " ")
  }
}


case class SQLStatmentMapping(statement: String) extends SQLMapping {

  def sqlContents: String = {
    statement
  }
}