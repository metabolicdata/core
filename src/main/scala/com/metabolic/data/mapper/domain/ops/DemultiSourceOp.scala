package com.metabolic.data.mapper.domain.ops

class DemultiSourceOp(
                      val idColumns: Seq[String],
                      val orderColumns: Seq[String],
                      val dateColumn: String,
                      val format: String,
                      val from: String,
                      val to: Option[String],
                      val endOfMonth: Option[Boolean]
                    ) extends SourceOp {

  val opName = "demulti"

}


