package com.metabolic.data.core.services.spark.transformations

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.DataFrame

class DropTransformation(val drops: Seq[String])
    extends Logging {

    def apply(): DataFrame => DataFrame = { df =>

      logger.info(s"Excluding $drops from SELECT *")

      df
        .drop(drops:_*)

    }

}
