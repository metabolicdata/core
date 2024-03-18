package com.metabolic.data.core.domain

import com.amazonaws.regions.Regions
import com.metabolic.data.mapper.domain.io.EngineMode
import com.typesafe.config.ConfigFactory

abstract class CoreConfig(val defaults: Defaults = Defaults(ConfigFactory.load()),
                          val environment: Environment = Environment("", EngineMode.Batch, "", false, "","",
                            Regions.fromName("eu-central-1"), Option.empty, Option.empty))