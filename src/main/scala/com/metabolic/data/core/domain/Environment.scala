package com.metabolic.data.core.domain
import com.metabolic.data.mapper.domain.io.EngineMode.EngineMode

case class Environment(name: String,
                       mode: EngineMode,
                       baseCheckpointLocation: String,
                       crawl: Boolean,
                       dbName: String,
                       iamRole: String,
                       atlanToken: Option[String],
                       atlanBaseUrl: Option[String],
                       historical: Boolean = false,
                       autoSchema: Boolean = false,
                       namespaces: Seq[String] = Seq.empty,
                       infix_namespaces: Seq[String] = Seq.empty,
                       enableJDBC: Boolean = false,
                       queryOutputLocation: String = "")
