package com.metabolic.data.mapper.services

import com.metabolic.data.mapper.domain.io.IOFormat

import com.typesafe.config.{Config => HoconConfig}

trait FormatParser {

  def parseFormat(source: HoconConfig): IOFormat.IOFormat = {
    if(source.hasPathOrNull("format")) {
      val stringFormat = source.getString("format").toLowerCase.trim
      try {
        IOFormat.withName(stringFormat)
      } catch {
        case _ : NoSuchElementException =>
          throw new UnsupportedOperationException(s"$stringFormat is not supported. Available formats are ${IOFormat.values}")
      }
    } else {
      IOFormat.TABLE
    }

  }

}
