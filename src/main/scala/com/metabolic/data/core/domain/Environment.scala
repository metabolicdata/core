package com.metabolic.data.core.domain
import com.amazonaws.regions.Regions
import com.metabolic.data.mapper.domain.io.EngineMode.EngineMode

case class Environment(name: String,
                       mode: EngineMode,
                       baseCheckpointLocation: String,
                       crawl: Boolean,
                       dbName: String,
                       iamRole: String,
                       region: Regions,
                       atlanToken: Option[String],
                       atlanBaseUrlDataLake: Option[String],
                       atlanBaseUrlConfluent: Option[String],
                       historical: Boolean = false,
                       autoSchema: Boolean = false,
                       namespaces: Seq[String] = Seq.empty,
                       infix_namespaces: Seq[String] = Seq.empty,
                       enableJDBC: Boolean = false,
                       queryOutputLocation: String = "")
