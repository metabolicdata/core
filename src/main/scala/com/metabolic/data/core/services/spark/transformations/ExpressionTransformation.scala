package com.metabolic.data.core.services.spark.transformations

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.DataFrame

class ExpressionTransformation(val expressions: Seq[String])
    extends Logging {

    def apply(): DataFrame => DataFrame = { df =>

      val allExpressions = Seq("*") ++ expressions

      logger.info(s"Applied SELECT expression $allExpressions")

      df
        .selectExpr(allExpressions:_*)

    }

}
